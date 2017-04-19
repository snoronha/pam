package lib

import (
	"fmt"
	"io"
	"os"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
)

func GetAWSService(region string) *s3.S3 {
	sess := session.Must(session.NewSession(&aws.Config{Region: aws.String(region)}))
	svc  := s3.New(sess)
	return svc
}

func GetAWSObjectNames(svc *s3.S3, bucket string, maxKeys int64, prefix string) []string {
	objects := make([]string, 0)
	params  := &s3.ListObjectsInput{ Bucket: aws.String(bucket), MaxKeys: aws.Int64(maxKeys), Prefix: aws.String(prefix) }
	resp, err := svc.ListObjects(params)
	if err != nil {
		fmt.Println(err.Error())
		return objects
	}
	for _, key := range resp.Contents {
		objects = append(objects, *key.Key)
	}
	return objects
}

func GetAWSFile(svc *s3.S3, bucket string, fileName string, ofileName string) {
	// fmt.Println("Loading " + fileName)
	object, err := svc.GetObject(&s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(fileName),})
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	file, err := os.Create(ofileName)
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	if _, err := io.Copy(file, object.Body); err != nil {
		fmt.Println(err.Error())
		return
	}
	object.Body.Close()
	file.Close()
}
