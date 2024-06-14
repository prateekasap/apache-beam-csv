## Please perform the following steps before running the program:

This is a data migration pipeline that reads data from a CSV file, decodes it from Shift-JIS encoding to UTF-8, and writes the decoded data to an S3 bucket.

## Prerequisites
Before running this pipeline, make sure you have the following:

JRE >= 11 installed

## Notes

The pipeline uses the DirectRunner for local execution.

- In the file `aws.properties` add your aws credentials.
- In the file `file_path.properties` add your csv file paths.

The files are under the `src/main/resources` folder.
