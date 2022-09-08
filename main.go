package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"regexp"
	"sort"
	"strings"
	"sync"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

var VALID_FORMATS = []string{"json", "pretty-json"}
var version = "development"

type object struct {
	Key       string `json:"Key"`
	VersionId string `json:"VersionId"`
}

func newObject(key, versionId string) object {
	return object{Key: key, VersionId: versionId}
}

type objectList struct {
	ObjectCount   int64                   `json:"Length"`
	Objects       []object                `json:"Objects"`
	DeleteMarkers []*s3.DeleteMarkerEntry `json:"DeleteMarkers"`
}

func newObjectList() *objectList {
	return &objectList{
		ObjectCount:   0,
		Objects:       make([]object, 0),
		DeleteMarkers: make([]*s3.DeleteMarkerEntry, 0),
	}
}

func (objList *objectList) add(key, versionId string) {
	objList.ObjectCount++
	objList.Objects = append(objList.Objects, newObject(key, versionId))
}

func (objList *objectList) appendDeleteMarkers(deleteMarkers []*s3.DeleteMarkerEntry) {
	objList.ObjectCount = objList.ObjectCount + int64(len(deleteMarkers))
	objList.DeleteMarkers = append(objList.DeleteMarkers, deleteMarkers...)
}

func (objList *objectList) toString(format string) string {
	switch format {
	case "json":
		return objList.toJSON(false)
	case "pretty-json":
		return objList.toJSON(true)
	}
	// We should never get here as formats are checked BEFORE execution.
	return ""
}

func (objList *objectList) toJSON(pretty bool) string {
	if pretty {
		b, _ := json.MarshalIndent(objList, "", "  ")
		return string(b)
	} else {
		b, _ := json.Marshal(objList)
		return string(b)
	}
}

func contains(list []string, matcher string) bool {
	for _, i := range list {
		if i == matcher {
			return true
		}
	}
	return false
}

func main() {
	flagBucketName := flag.String("bucket-name", "", "Name of the bucket to empty.")
	flagProfile := flag.String("profile", "", "AWS Profile to use, if there is one.")
	flagAWSRegion := flag.String("aws-region", "", "The region for the aws connection. If set it will override what is set in AWS_REGION. If there is no AWS_REGION, then eu-west-1 will be used.")
	flagFormat := flag.String("format", "pretty-json", fmt.Sprintf("If -dry-run or -show-objects is used, the format of the output, %s are available.", strings.Join(VALID_FORMATS, ",")))
	flagDryRun := flag.Bool("dry-run", false, "Show versions to be deleted.")
	flagShowObjects := flag.Bool("show-objects", false, "Show the objects before attempting to delete them.")
	flagVersion := flag.bool("v", false, "Print the version.")

	flag.Parse()

	if *flagVersion {
		fmt.Println(version)
		return
	}

	if *flagBucketName == "" {
		fmt.Println("No Bucket name was given.")
		flag.PrintDefaults()
		os.Exit(1)
	}

	if *flagAWSRegion != "" {
		os.Setenv("AWS_REGION", *flagAWSRegion)
	}
	if _, set := os.LookupEnv("AWS_REGION"); !set {
		os.Setenv("AWS_REGION", "eu-west-1")
	}

	if !contains(VALID_FORMATS, *flagFormat) {
		fmt.Printf("%s is not a valid format.", *flagFormat)
		flag.PrintDefaults()
		os.Exit(1)
	}

	awsSession, err := setupAwsSession(*flagProfile)
	if err != nil {
		fmt.Printf("There was an error getting your AWS Creds. Error: %s", err)
		os.Exit(1)
	}
	awsSession.Config.CredentialsChainVerboseErrors = aws.Bool(true)

	list, err := listObjects(awsSession, *flagBucketName)
	if err != nil {
		fmt.Printf("There was an error listing the objects for your specified bucket '%s'.\nError: %s\n", *flagBucketName, err)
		os.Exit(1)
	}

	if *flagDryRun || *flagShowObjects {
		fmt.Println(list.toString(*flagFormat))
	}

	if *flagDryRun {
		return
	}

	rawErrors, err := deleteObjects(awsSession, *flagBucketName, *list)
	if err != nil {
		fmt.Printf("There was an error deleting objects. Error: %s.", err)
		fmt.Println("Raw Request Errors:")
		for _, e := range rawErrors {
			fmt.Println(e)
		}
	}
}

func setupAwsSession(profile string) (*session.Session, error) {
	if profile != "" {
		return session.NewSessionWithOptions(session.Options{
			Profile:           profile,
			SharedConfigState: session.SharedConfigEnable,
		})
	}

	return session.NewSession()
}

func listObjects(awsSession *session.Session, bucket string) (*objectList, error) {
	s3Handler := s3.New(awsSession)

	wg := sync.WaitGroup{}
	objectHopper := make(chan s3.ListObjectVersionsOutput, 1)
	returnValue := newObjectList()
	wg.Add(1)
	go func(hopper chan s3.ListObjectVersionsOutput) {
		defer wg.Done()
		for page := range hopper {
			for _, obj := range page.Versions {
				returnValue.add(aws.StringValue(obj.Key), aws.StringValue(obj.VersionId))
			}
			returnValue.appendDeleteMarkers(page.DeleteMarkers)
		}
	}(objectHopper)

	err := s3Handler.ListObjectVersionsPages(&s3.ListObjectVersionsInput{
		Bucket: aws.String(bucket),
	}, func(page *s3.ListObjectVersionsOutput, lastPage bool) bool {
		objectHopper <- *page
		return true
	})

	close(objectHopper)
	wg.Wait()

	if err != nil {
		return newObjectList(), err
	}

	if returnValue.ObjectCount == 0 {
		return newObjectList(), fmt.Errorf("no objects found")
	}
	return returnValue, nil
}

func deleteObjects(awsSession *session.Session, bucketName string, objects objectList) ([]string, error) {
	s3Handler := s3.New(awsSession)

	s3ObjectsRaw := []*s3.ObjectIdentifier{}
	s3DirsRaw := []*s3.ObjectIdentifier{}

	dirMatcher := regexp.MustCompile("/$")
	for _, obj := range objects.Objects {
		currentObject := &s3.ObjectIdentifier{
			Key:       aws.String(obj.Key),
			VersionId: aws.String(obj.VersionId),
		}

		if dirMatcher.MatchString(obj.Key) {
			s3DirsRaw = append(s3DirsRaw, currentObject)
		} else {
			s3ObjectsRaw = append(s3ObjectsRaw, currentObject)
		}
	}

	for _, dm := range objects.DeleteMarkers {
		currentObject := &s3.ObjectIdentifier{
			Key:       dm.Key,
			VersionId: dm.VersionId,
		}
		s3ObjectsRaw = append(s3ObjectsRaw, currentObject)
	}

	// Sort the objects so that we can delete the deepest directories first
	sort.SliceStable(s3DirsRaw, func(i, j int) bool {
		a := strings.Count(aws.StringValue(s3DirsRaw[i].Key), "/")
		b := strings.Count(aws.StringValue(s3DirsRaw[j].Key), "/")
		return a < b
	})

	deletePacks := []*s3.Delete{}
	deletePacks = append(deletePacks, &s3.Delete{})

	index := 0

	if len(s3ObjectsRaw) > 0 {
		if len(s3ObjectsRaw) <= 1000 {
			deletePacks = append(deletePacks, &s3.Delete{Objects: s3ObjectsRaw})
		} else {
			counter := 1
			max := 1000
			for _, o := range s3ObjectsRaw {
				if counter <= max {
					counter++
					deletePacks[index].Objects = append(deletePacks[index].Objects, o)
				} else {
					deletePacks = append(deletePacks, &s3.Delete{})
					index++
					counter = 1
					deletePacks[index].Objects = append(deletePacks[index].Objects, o)
				}
			}
		}
	}

	if len(s3DirsRaw) > 0 {
		if len(s3DirsRaw) <= 1000 {
			deletePacks = append(deletePacks, &s3.Delete{Objects: s3DirsRaw})
		} else {
			counter := 1
			max := 1000
			for _, o := range s3DirsRaw {
				if counter <= max {
					counter++
					deletePacks[index].Objects = append(deletePacks[index].Objects, o)
				} else {
					deletePacks = append(deletePacks, &s3.Delete{})
					index++
					counter = 1
					deletePacks[index].Objects = append(deletePacks[index].Objects, o)
				}
			}
		}
	}

	for _, deletePack := range deletePacks {
		objectsToDelete := s3.DeleteObjectsInput{
			Bucket: aws.String(bucketName),
			Delete: deletePack,
		}
		if len(deletePack.Objects) == 0 {
			continue
		}
		fmt.Printf("Attemting to delete %d objects\n", len(deletePack.Objects))
		out, err := s3Handler.DeleteObjects(&objectsToDelete)
		if err != nil {
			errs := []string{}
			for _, e := range out.Errors {
				errs = append(errs, e.String())
			}

			return errs, err
		}
	}

	return []string{}, nil
}
