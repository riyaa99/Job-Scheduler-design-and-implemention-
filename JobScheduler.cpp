/*
 * Job Scheduling System
 * Simulates distributed job scheduling system with multiple worker nodes.
 * Implements different queueing & allocation policies to analyze perf.
 */

#include <iostream>
#include <queue>
#include <vector>
#include <string>
#include <thread>
#include <mutex>
#include <algorithm>
#include <sstream>
#include <fstream>
#include <iomanip> 
#include <random>

using namespace std;

const int WORKER_NODES = 128;     // Total number of worker nodes available
const int CORES_PER_NODE = 24;    // CPU cores per node
const int RAM_PER_NODE_GB = 64;   // RAM in GB per node

/**
 * Job class represents a computational task with resource requirements
 * Each job has specific CPU, memory and exec time requirements
 */
class Job {
public:
    int jobId;          // Unique identifier for job
    int arrivalTime;    // Day when job arrives
    int timeHour;       // Hour when job arrives
    int coresReq;       // Number of CPU cores required
    int memReq;         // Amount of memory required in GB
    int execTime;       // Execution time required in hrs
    int grossValue;     // Total resource usage (cores * mem * time)

    // Constructor with params to initialize job
    Job(int id, int arrival, int cores, int mem, int exec) : 
        jobId(id), arrivalTime(arrival), timeHour(arrival), coresReq(cores), memReq(mem), execTime(exec) {
        grossValue = exec * cores * mem;  
    }

    // Default constructor
    Job() : jobId(0), arrivalTime(0), timeHour(0), coresReq(0), memReq(0), execTime(0), grossValue(0) {}

    // Prints job details in formatted manner
    void printDetails() const {
        cout << "JobId: " << setw(5) << jobId << setw(2)
             << " Arrival Day: " << setw(2) << arrivalTime 
             << " Time Hour: " << setw(2) << timeHour 
             << " MemReq: " << setw(3) << memReq 
             << " CPUReg: " << setw(3) << coresReq 
             << " ExeTime: " << setw(3) << execTime << '\n';
    }
};

/**
 * WorkerNode class represents a single computational node in sys
 * Each node has CPU cores and memory that can be allocated to jobs
 */
class WorkerNode {
public:
    int nodeId;             // Unique identifier for node
    int availableCores;     // Number of CPU cores currently available
    int availableMem;       // Amount of memory currently available in GB
    bool isBusy;           // Flag indicating if node is processing a job

    // Constructor initializes node with full resources
    WorkerNode(int id) : nodeId(id), availableCores(CORES_PER_NODE), availableMem(RAM_PER_NODE_GB), isBusy(false) {}

    // Checks if node can accommodate given job's resource requirements
    bool canAllocate(const Job& job) const {
        return availableCores >= job.coresReq && availableMem >= job.memReq;
    }

    // Allocates resources to a job
    void allocateJob(const Job& job) {
        availableCores -= job.coresReq;
        availableMem -= job.memReq;
        isBusy = true;
    }

    // Releases resources after job completion
    void releaseResources(const Job& job) {
        availableCores += job.coresReq;
        availableMem += job.memReq;
        isBusy = false;
    }
};

/**
 * MasterScheduler class manages entire job scheduling system
 * Implements different scheduling policies & tracks system metrics
 */
class MasterScheduler {
private:
    vector<WorkerNode> workerNodes;   // Collection of worker nodes
    vector<Job> jobQueue;             // Queue of waiting jobs
    int totalCpuUsage;               // Total CPU cores in use
    int totalMemUsage;               // Total memory in use
    int completedJobs;               // Counter for completed jobs

public:
    // Initialize scheduler with specified number of worker nodes
    MasterScheduler(int numWorkers) : totalCpuUsage(0), totalMemUsage(0), completedJobs(0) {
        for (int i = 0; i < numWorkers; i++) {
            workerNodes.emplace_back(i);
        }
    }

    int getTotalCpuUsage() const { return totalCpuUsage; }
    int getTotalMemUsage() const { return totalMemUsage; }
    int getCompletedJobs() const { return completedJobs; }


    // Add new job to queue
    void addJob(const Job& job) {
        jobQueue.push_back(job);
    }

    /**
     * Implements different queueing policies:
     * 1. FCFS
     * 2. Smallest Job First (based on gross resource usage)
     * 3. Shortest Duration First
     */
    void orderQueue(int policy) {
        if (policy == 1) {
            // FCFS - no reordering needed
        } else if (policy == 2) {
            // Smallest job first
            sort(jobQueue.begin(), jobQueue.end(), [](const Job& a, const Job& b) {
                return a.grossValue < b.grossValue;
            });
        } else if (policy == 3) {
            // Short duration job first
            sort(jobQueue.begin(), jobQueue.end(), [](const Job& a, const Job& b) {
                return a.execTime < b.execTime;
            });
        }
    }

    /**
     * Implements different allocation policies:
     * 1. First Fit: Allocate to first available node
     * 2. Best Fit: Allocate to node with smallest sufficient resources
     * 3. Worst Fit: Allocate to node with largest available resources
     */
    WorkerNode* allocateWorker(const Job& job, int policy) {
        WorkerNode* selectedNode = nullptr;

        for (auto& node : workerNodes) {
            if (node.canAllocate(job)) {
                if (policy == 1) { // First Fit: first node that can be allocated
                    selectedNode = &node;
                    break;
                } else if (policy == 2) { // Best Fit: smallest node that's big enough
                    if (!selectedNode || (node.availableCores + node.availableMem) < 
                        (selectedNode->availableCores + selectedNode->availableMem)) {
                        selectedNode = &node;
                    }
                } else if (policy == 3) { // Worst Fit: largest node that can be allocated
                    if (!selectedNode || (node.availableCores + node.availableMem) > 
                        (selectedNode->availableCores + selectedNode->availableMem)) {
                        selectedNode = &node;
                    }
                }
            }
        }

        if (selectedNode) {
            selectedNode->allocateJob(job);
            // Track CPU and memory usage separately
            totalCpuUsage += job.coresReq;
            totalMemUsage += job.memReq;
            completedJobs++;
        }
        return selectedNode;
    }

    /**
     * Simulates one day of job processing with given policies
     * Introduces variability in resource availability
     */
    void simulateDay(int queuePolicy, int allocPolicy) {
        orderQueue(queuePolicy);
        vector<Job> unallocatedJobs;

        // Reset daily usage metrics
        totalCpuUsage = 0;
        totalMemUsage = 0;
        completedJobs = 0;

        for (auto& node : workerNodes) {
            // Reset node resources to full capacity at the start of each day
            node.availableCores = CORES_PER_NODE;
            node.availableMem = RAM_PER_NODE_GB;
        }

        for (const auto& job : jobQueue) {
            if (!allocateWorker(job, allocPolicy)) {
                unallocatedJobs.push_back(job);
            }
        }

        jobQueue = unallocatedJobs;
    }

    // Prints perf metrics for current day
    void printDailyMetrics(int day, int queuePolicy, int allocPolicy) const {
        // Calculate CPU utilization as percentage of total available cores across nodes
        double cpuUtilization = (totalCpuUsage * 100.0) / (WORKER_NODES * CORES_PER_NODE);
        
        // Calculate memory utilization as percentage of total available RAM across nodes
        double memUtilization = (totalMemUsage * 100.0) / (WORKER_NODES * RAM_PER_NODE_GB);
        
        // Print comprehensive daily stats
        cout << "Day " << day << " - Queue Policy: " << queuePolicy
             << ", Allocation Policy: " << allocPolicy << "\n"
             << "Completed Jobs: " << completedJobs << "\n"
             << "CPU Utilization: " << fixed << setprecision(2) << cpuUtilization << "%"
             << " (" << totalCpuUsage << "/" << WORKER_NODES * CORES_PER_NODE << " cores)\n"
             << "Memory Utilization: " << fixed << setprecision(2) << memUtilization << "%"
             << " (" << totalMemUsage << "/" << WORKER_NODES * RAM_PER_NODE_GB << " GB)\n\n";
    }
};

/**
 * Reads job specifications from input file in its given format
 */
vector<Job> readJobs(const string& filename) {



    ifstream file(filename);
    vector<Job> jobs;
    string line;

    if (!file) {
        cerr << "Error opening file: " << filename << endl;
        return jobs;
    }

    while (getline(file, line)) {
        if (line.empty()) continue;  // Skip empty lines

        int jobId, arrivalDay, timeHour, memReq, cpuReq, exeTime;
        string jobIdStr, arrivalStr, dayStr, timeStr, hourStr, memReqStr, cpuReqStr, exeTimeStr;

        istringstream iss(line);
        
        // Parse each component of the job specification
        if (iss >> jobIdStr >> jobId 
            >> arrivalStr >> dayStr >> arrivalDay 
            >> timeStr >> hourStr >> timeHour
            >> memReqStr >> memReq 
            >> cpuReqStr >> cpuReq 
            >> exeTimeStr >> exeTime) {

            // Verify correct format of input line
            if (jobIdStr == "JobId:" &&
                arrivalStr == "Arrival" && dayStr == "Day:" &&
                timeStr == "Time" && hourStr == "Hour:" &&
                memReqStr == "MemReq:" &&
                cpuReqStr == "CPUReg:" &&
                exeTimeStr == "ExeTime:") {
                
                jobs.emplace_back(jobId, arrivalDay, cpuReq, memReq, exeTime);
            }
        }
    }

    return jobs;
}

/**
 * Runs simulation for specified number of days testing all policy combinations
 * Introduces variability by randomizing policy selection
 */
void runSimulation(MasterScheduler& scheduler, int numDays) {
    random_device rd;
    mt19937 gen(rd());
    uniform_int_distribution<> policyDist(1, 3); // Randomly select policy

    for (int day = 1; day <= numDays; day++) {
        int queuePolicy = policyDist(gen);
        int allocPolicy = policyDist(gen);

        // Run simulation for this policy combination
        scheduler.simulateDay(queuePolicy, allocPolicy);
        
        // Print metrics to console
        scheduler.printDailyMetrics(day, queuePolicy, allocPolicy);
    }
}

/**
 * Writes simulation metrics to CSV file for analysis
 */
void writeMetricsToCSV(MasterScheduler& scheduler, int numDays) {
    ofstream metricsFile("scheduling_metrics.csv");
    
    // Write CSV header
    metricsFile << "Day,Queue Policy,Alloc Policy,CPU Utilization,Memory Utilization\n";

    for (int day = 1; day <= numDays; day++) {
        for (int queuePolicy = 1; queuePolicy <= 3; queuePolicy++) {
            for (int allocPolicy = 1; allocPolicy <= 3; allocPolicy++) {
                // Simulate the day with the current policy combination
                scheduler.simulateDay(queuePolicy, allocPolicy);

                // Calculate utilization metrics
                double cpuUtil = (scheduler.getTotalCpuUsage() * 100.0) / (WORKER_NODES * CORES_PER_NODE);
                double memUtil = (scheduler.getTotalMemUsage() * 100.0) / (WORKER_NODES * RAM_PER_NODE_GB);
                
                // Write metrics to CSV
                metricsFile << day << ","
                            << queuePolicy << ","
                            << allocPolicy << ","
                            << fixed << setprecision(2) << cpuUtil << ","
                            << memUtil << "\n";
            }
        }
    }
    
    metricsFile.close();
    cout << "\nMetrics have been written to scheduling_metrics.csv\n";
}




/**
 * Main function - Entry point of program
 * Sets up scheduling system and runs simulation
 */
int main() {
    // Create master scheduler with 128 worker nodes
    MasterScheduler scheduler(WORKER_NODES);
    
    // Read jobs from input file
    cout << "Reading jobs from dataset (JobArrival.txt)..." << endl;
    vector<Job> jobs = readJobs("JobArrival.txt");
    
    // Ask user if they want to print jobs to console
    char printJobs;
    cout << "Do you want to print the jobs to the console? (y/n): ";
    cin >> printJobs;

    // Add all jobs to scheduler
    for (const Job& job : jobs) {
        scheduler.addJob(job);
        if (printJobs == 'y' || printJobs == 'Y') {
            job.printDetails();
        }
    }

    int n = 0;
    cout << "Enter number of days to simulate: ";
    cin >> n;
    // Run simulation for n days
    runSimulation(scheduler, n);

    // Add this line to generate the CSV file
    writeMetricsToCSV(scheduler, n);

    // Generate random test jobs
    // generateTestJobs(scheduler, 10000);



    return 0;
}