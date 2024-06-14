package org.example;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.aws.options.AwsOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import java.util.Properties;



public class Main {

    public static void main(String[] args) {

        Properties confs = new Properties();
        // Create pipeline options
        AwsOptions options = PipelineOptionsFactory.create().as(AwsOptions.class);

        options.setAwsCredentialsProvider(new AWSStaticCredentialsProvider(new BasicAWSCredentials(confs.getProperty("aws.accessKeyId"), "aws.secretAccessKey")));
        options.setAwsRegion("aws.region");

        options.setRunner(DirectRunner.class);
        // Create the pipeline
        Pipeline pipeline = Pipeline.create(options);

        String inputPath  = confs.getProperty("input.path");
        String outputPath = confs.getProperty("output.path");


        PCollection<FileIO.ReadableFile> files = pipeline.apply("MatchFiles", FileIO.match().filepattern(inputPath))
                .apply("ReadMatchedFiles", FileIO.readMatches());
        files.apply("ProcessAndWriteFiles", ParDo.of(new ProcessAndWriteFilesFn(outputPath)));

        pipeline.run().waitUntilFinish();
    }

    static class ProcessAndWriteFilesFn extends DoFn<FileIO.ReadableFile, Void> {
        private final String outputPath;

        public ProcessAndWriteFilesFn(String outputPath) {
            this.outputPath = outputPath;
        }

        @ProcessElement
        public void processElement(@Element FileIO.ReadableFile file, OutputReceiver<Void> out) {
            String inputFile = file.getMetadata().resourceId().toString();
            String fileName = inputFile.substring(inputFile.lastIndexOf('/') + 1);
            String destinationPath = outputPath + fileName;

            try {
                Properties confs = new Properties();
                String content = new String(file.readFullyAsBytes(), Charset.forName("Shift_JIS"));
                String decodedContent = new String(content.getBytes(StandardCharsets.UTF_8), StandardCharsets.UTF_8);

                AwsOptions options = PipelineOptionsFactory.create().as(AwsOptions.class);
                options.setAwsCredentialsProvider(new AWSStaticCredentialsProvider(new BasicAWSCredentials(confs.getProperty("aws.accessKeyId"), "aws.secretAccessKey")));
                options.setAwsRegion("aws.region");

                options.setRunner(DirectRunner.class);

                Pipeline subPipeline = Pipeline.create(options);
                PCollection<String> singleFileCollection = subPipeline.apply(Create.of(decodedContent)).setCoder(StringUtf8Coder.of());
                singleFileCollection.apply("WriteFile", TextIO.write().to(destinationPath).withSuffix(".csv").withoutSharding());
                subPipeline.run().waitUntilFinish();
            } catch (Exception e) {
            }
        }
    }
}


