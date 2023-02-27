package process

import (
	"log"
	"os"
)

func ChangeWorkingDirectory(directory *string) {
	// Verify whether the directory entry is set, if it is not set, then use the current working directory, if it is
	// set, attempt to change the current working directory to the given directory.
	if directory != nil && len(*directory) > 0 {
		err := os.Chdir(*directory)
		if err != nil {
			log.Fatalln("Could not change working directory to:", *directory, "error:", err.Error())
		}
	}
	// Check if the current working directory can be sensibly accessed
	workingDirectory, err := os.Getwd()
	if err != nil {
		log.Fatalln("Environment not safe, could not retrieve working directory, error:", err.Error())
	}
	// Print a friendly message to the user which working directory is used
	log.Println("Working directory set to:", workingDirectory)
}
