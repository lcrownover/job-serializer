package main

// go to basedir
// find all directories that start with "20"
// for each of those directories, count how many files are in the directory
// if there are no files, skip it
// if DATE.json exists
//   check if the total_jobs matches the number of files in the directory
//   if it does, skip it
// else
//   create a DATE.json file for that day
//    { "date": "2021-01-01",
//      "total_jobs": len(files),
//      "parsed_jobs": 0,
//      "jobs": [
//         {
//           "job_id": "1234",
//           "job_name": "job1",
//           "user_id": "user1",
//           "group_id": "group1",
//           "submit_time": "2021-01-01 00:00:00",
//           "start_time": "2021-01-01 00:00:00",
//           "end_time": "2021-01-01 00:00:00",
//           "partition": "partition1",
//           "num_nodes": "1"
//         },
//      ]}
//   for each file in the directory
//    parse the file
//    add the parsed job to the jobs array
//    increment parsed_jobs

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strings"
	"sync"
)

type JobDay struct {
	Filepath       string `json:"filepath"`
	OutputDir      string `json:"output_dir"`
	OutputFilePath string `json:"output_filepath"`
	Date           string `json:"date"`
	TotalJobs      int    `json:"total_jobs"`
	ParsedJobs     int    `json:"parsed_jobs"`
	Jobs           []Job  `json:"jobs"`
}

func NewJobDay(path string, outputDir string) *JobDay {
	// get the number of files in the directory
	entries, err := os.ReadDir(path)
	if err != nil {
		writeStderr(err.Error())
		os.Exit(1)
	}

	totalJobs := len(entries)
	datestamp := strings.Split(path, "/")[len(strings.Split(path, "/"))-1]
	ymd := convertDatestampToYMD(datestamp)

	outputFilePath := outputDir + "/" + ymd + ".json"

	return &JobDay{
		Filepath:       path,
		OutputDir:      outputDir,
		OutputFilePath: outputFilePath,
		Date:           ymd,
		TotalJobs:      totalJobs,
		ParsedJobs:     0,
		Jobs:           []Job{},
	}
}

// Scan the directory for job files
// If the data.json file exists, check if the total_jobs matches the number of files in the directory
// If it does, skip it
// Parse the job files in a waitgroup and add them to the Jobs array
// Increment the ParsedJobs counter
func (jd *JobDay) Scan(force bool) {
	entries, err := os.ReadDir(jd.Filepath)
	if err != nil {
		writeStderr(err.Error())
		os.Exit(1)
	}
	// ignore the skip logic if force is used
	if !force {
		df, err := os.Open(jd.OutputFilePath)
		if err != nil {
			if !os.IsNotExist(err) {
				writeStderr(err.Error())
				os.Exit(1)
			}
		}
		if df != nil {
			defer df.Close()
			var data JobDay
			json.NewDecoder(df).Decode(&data)
			if data.TotalJobs == len(entries) {
				fmt.Printf("Skipping %s\n", jd.Filepath)
				return
			}
		}
	}

	fmt.Printf("Parsing %s\n", jd.Filepath)
	var jobChan = make(chan Job, 8000)
	var wg sync.WaitGroup
	for _, entry := range entries {
		wg.Add(1)
		go func(entry os.DirEntry, jobChan chan Job) {
			defer wg.Done()
			entrypath := jd.Filepath + "/" + entry.Name()
			job, err := parseJobFile(entrypath)
			if err != nil {
				writeStderr(err.Error())
				os.Exit(1)
			}
			jobChan <- *job
		}(entry, jobChan)
	}
	wg.Wait()
	close(jobChan)
	for job := range jobChan {
        if job.JobId == "" {
            continue
        }
		jd.Jobs = append(jd.Jobs, job)
		jd.ParsedJobs++
	}
}

// Write the JobDay struct to a file
func (jd *JobDay) Write() {
	// create the output directory if it doesn't exist
	if _, err := os.Stat(jd.OutputFilePath); os.IsNotExist(err) {
		err := os.MkdirAll(jd.OutputDir, 0700)
		if err != nil {
			writeStderr(err.Error())
			os.Exit(1)
		}
	}
	file, err := os.Create(jd.OutputFilePath)
	if err != nil {
		writeStderr(err.Error())
		os.Exit(1)
	}
	defer file.Close()
	json, err := json.Marshal(jd)
	if err != nil {
		writeStderr(err.Error())
		os.Exit(1)
	}
	file.Write(json)
}

type Job struct {
	JobId           string `json:"job_id"`
	JobName         string `json:"job_name"`
	JobState        string `json:"job_state"`
	Account         string `json:"account"`
	UserId          string `json:"user_id"`
	GroupId         string `json:"group_id"`
	QOS             string `json:"qos"`
	Requeue         string `json:"requeue"`
	Restarts        string `json:"restarts"`
	BatchFlag       string `json:"batch_flag"`
	Reboot          string `json:"reboot"`
	ExitCode        string `json:"exit_code"`
	DerivedExitCode string `json:"derived_exit_code"`
	RunTime         string `json:"run_time"`
	TimeLimit       string `json:"time_limit"`
	TimeMin         string `json:"time_min"`
	EligibleTime    string `json:"eligible_time"`
	AccrueTime      string `json:"accrue_time"`
	SuspendTime     string `json:"suspend_time"`
	NodeList        string `json:"node_list"`
	BatchHost       string `json:"batch_host"`
	SubmitTime      string `json:"submit_time"`
	StartTime       string `json:"start_time"`
	EndTime         string `json:"end_time"`
	Partition       string `json:"partition"`
	NumNodes        string `json:"num_nodes"`
	NumCPUs         string `json:"num_cpus"`
	NumTasks        string `json:"num_tasks"`
	TRES            string `json:"tres"`
	Priority        string `json:"priority"`
	Reason          string `json:"reason"`
	MinCPUsNode     string `json:"min_cpus_node"`
	MinMemoryCPU    string `json:"min_memory_cpu"`
	Command         string `json:"command"`
	WorkDir         string `json:"work_dir"`
	Power           string `json:"power"`
	ExcNodeList     string `json:"exc_node_list"`
	StdErr          string `json:"std_err"`
	StdIn           string `json:"std_in"`
	StdOut          string `json:"std_out"`
}

func parseJobFile(filepath string) (*Job, error) {
	var job Job
	file, err := os.Open(filepath)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		job = parseJobFileLine(&job, line)
	}
	return &job, nil
}

func safeGetProperty(pair []string) string {
	if len(pair) < 2 {
		return ""
	}
	return pair[1]
}

func parseJobFileLine(job *Job, line string) Job {
	line = strings.TrimSpace(line)
	pairs := strings.Split(line, " ")
	for _, pair := range pairs {
		kv := strings.SplitN(pair, "=", 2)
		switch kv[0] {
		case "JobId":
            // if this is the second time seeing the JobID, just return the job
            if job.JobId != "" {
                return *job
            }
			job.JobId = safeGetProperty(kv)
		case "JobName":
			job.JobName = safeGetProperty(kv)
		case "UserId":
			job.UserId = safeGetProperty(kv)
		case "GroupId":
			job.GroupId = safeGetProperty(kv)
		case "SubmitTime":
			job.SubmitTime = safeGetProperty(kv)
		case "StartTime":
			job.StartTime = safeGetProperty(kv)
		case "EndTime":
			job.EndTime = safeGetProperty(kv)
		case "Partition":
			job.Partition = safeGetProperty(kv)
		case "NumNodes":
			job.NumNodes = safeGetProperty(kv)
		case "NumCPUs":
			job.NumCPUs = safeGetProperty(kv)
		case "NumTasks":
			job.NumTasks = safeGetProperty(kv)
		case "TRES":
			job.TRES = safeGetProperty(kv)
		case "Priority":
			job.Priority = safeGetProperty(kv)
		case "Reason":
			job.Reason = safeGetProperty(kv)
		case "MinCPUsNode":
			job.MinCPUsNode = safeGetProperty(kv)
		case "MinMemoryCPU":
			job.MinMemoryCPU = safeGetProperty(kv)
		case "Command":
			job.Command = safeGetProperty(kv)
		case "WorkDir":
			job.WorkDir = safeGetProperty(kv)
		case "Power":
			job.Power = safeGetProperty(kv)
		case "JobState":
			job.JobState = safeGetProperty(kv)
		case "Account":
			job.Account = safeGetProperty(kv)
		case "QOS":
			job.QOS = safeGetProperty(kv)
		case "Requeue":
			job.Requeue = safeGetProperty(kv)
		case "Restarts":
			job.Restarts = safeGetProperty(kv)
		case "BatchFlag":
			job.BatchFlag = safeGetProperty(kv)
		case "Reboot":
			job.Reboot = safeGetProperty(kv)
		case "ExitCode":
			job.ExitCode = safeGetProperty(kv)
		case "DerivedExitCode":
			job.DerivedExitCode = safeGetProperty(kv)
		case "RunTime":
			job.RunTime = safeGetProperty(kv)
		case "TimeLimit":
			job.TimeLimit = safeGetProperty(kv)
		case "TimeMin":
			job.TimeMin = safeGetProperty(kv)
		case "EligibleTime":
			job.EligibleTime = safeGetProperty(kv)
		case "AccrueTime":
			job.AccrueTime = safeGetProperty(kv)
		case "SuspendTime":
			job.SuspendTime = safeGetProperty(kv)
		case "NodeList":
			job.NodeList = safeGetProperty(kv)
		case "BatchHost":
			job.BatchHost = safeGetProperty(kv)
		case "StdErr":
			job.StdErr = safeGetProperty(kv)
		case "StdIn":
			job.StdIn = safeGetProperty(kv)
		case "StdOut":
			job.StdOut = safeGetProperty(kv)
		case "ExcNodeList":
			job.ExcNodeList = safeGetProperty(kv)
		}
	}
	return *job
}

func writeStderr(msg string) {
	fmt.Fprintf(os.Stderr, "Error: %s\n", msg)
}

func getJobFilepaths(datepath string) []string {
	entries, err := os.ReadDir(datepath)
	if err != nil {
		writeStderr(err.Error())
	}
	var jobFilepaths []string
	for _, entry := range entries {
		jobFilepaths = append(jobFilepaths, datepath+"/"+entry.Name())
	}
	return jobFilepaths
}

func convertDatestampToYMD(datestamp string) string {
	// datestamp is in the format YYYYMMDD
	// we want to convert it to YYYY-MM-DD
	return datestamp[0:4] + "-" + datestamp[4:6] + "-" + datestamp[6:8]
}

func convertYMDToDatestamp(ymd string) string {
	// ymd is in the format YYYY-MM-DD
	// we want to convert it to YYYYMMDD
	return ymd[0:4] + ymd[5:7] + ymd[8:10]
}

func getDirPaths(basepath string, ymd string) []string {
	var validEntries []string
	entries, err := os.ReadDir(basepath)
	if err != nil {
		writeStderr(err.Error())
		os.Exit(1)
	}
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), "20") {
			entryPath := basepath + "/" + entry.Name()
			if ymd == "" { // we're not looking for a specific datedir
				validEntries = append(validEntries, entryPath)
			}
			if ymd != "" {
				datestamp := convertYMDToDatestamp(ymd)
				if strings.Contains(entry.Name(), datestamp) {
					validEntries = append(validEntries, entryPath)
				}
			}
		}
	}
	return validEntries
}

func main() {
	var basedirFlag = flag.String("basedir", "", "path to directory containing job directories")
	var outputdirFlag = flag.String("outputdir", "", "path to directory for output files")
	var dateFlag = flag.String("date", "", "date to parse in the format YYYY-MM-DD")
	var forceFlag = flag.Bool("force", false, "force re-parsing of jobs")
	flag.Parse()

	if *basedirFlag == "" {
		writeStderr("basedir is required")
		flag.Usage()
		os.Exit(1)
	}

	if *outputdirFlag == "" {
		writeStderr("outputdir is required")
		flag.Usage()
		os.Exit(1)
	}

	paths := getDirPaths(*basedirFlag, *dateFlag)

	// this will iterate over the DAY directories in basedir
	// we iterate through days sequentially,
	// but we can parse jobs in parallel
	for _, path := range paths {
		// entry is a full path to a directory
		// inside this loop, we waitgroup over all the files in the day
		jobDay := NewJobDay(path, *outputdirFlag)
		jobDay.Scan(*forceFlag)
		jobDay.Write()
	}
}
