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
//    { "total_jobs": len(files), 
//      "parsed_jobs": 0, 
//      "jobs": [
//        {"propOne": "something", "propTwo": "somethingelse"},
//      ]}
//   for each file in the directory
//    parse the file 
//    add the parsed job to the jobs array
//    increment parsed_jobs

import (
	"bufio"
	"fmt"
	"os"
	"strings"
	"sync"
)

type Job struct {
	JobId      string `json:"job_id"`
	JobName    string `json:"job_name"`
	UserId     string `json:"user_id"`
	GroupId    string `json:"group_id"`
	SubmitTime string `json:"submit_time"`
	StartTime  string `json:"start_time"`
	EndTime    string `json:"end_time"`
	Partition  string `json:"partition"`
	NumNodes   string `json:"num_nodes"`
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
		kv := strings.Split(pair, "=")
		switch kv[0] {
		case "JobId":
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
		}
	}
	return *job
}

func writeStderr(msg string) {
	fmt.Fprintf(os.Stderr, msg)
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

func main() {
	entries, err := os.ReadDir("./")
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

    // output structure
    // {
    //   "2021": {
    //     "01": {
    //       "01": [
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
    //         {
    //           "job_id": "1235",
    //           "job_name": ".......",
    //         },
    //       ],
    //       "02": [
    //         {more}, 
    //         {jobs...}
    //       ],
    //     },
    //   "02": {
    //     "01": [],
    //     "02": [],
    //   },
    //  "2020": {
    //    "01": {
    //      "01": [],
    //      "02": [],
    //    },
    //    "02": {
    //      "01": [],
    //      "02": [],
    //    },
    //  },

    jobData := make(map[string][]Job, len(entries))
	jobFilepaths := make(map[string][]string, len(entries))

	var wg sync.WaitGroup
	for _, entry := range entries {
        // iterate over DAYS
		ymdDirName := entry.Name()
		if !strings.HasPrefix(ymdDirName, "20") {
            // ignore anything that's not a job dir
			continue
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
            
			var filepaths []string
			fps := getJobFilepaths(ymdDirName)

			filepaths = append(filepaths, fps...)
            jobFilepaths[ymdDirName] = filepaths
		}()
	}
	fmt.Println("Getting job filepaths...")
	wg.Wait()

    // can we just dump everything?
	for ymdStr, jobFilepath := range jobFilepaths {
        wg.Add(1)
        go func() {
            defer wg.Done()
            
        }()
	}
}
