package com.bachinalabs;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TypeDescriptors;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args ) {

        // Start by defining the options for the pipeline.
        // PipelineOptions options = PipelineOptionsFactory.as(PipelineOptions.class);
        PipelineOptionsFactory.register(RuntimeOptions.class);
        RuntimeOptions options = PipelineOptionsFactory.fromArgs(args)
                .withValidation()
                .as(RuntimeOptions.class);

        // Then create the pipeline.
        Pipeline p = Pipeline.create(options);

        p.apply("ReadLines", TextIO.read().from(options.getInputFile()))
                .apply(new SplitWords())
                .apply(new CountWords())
                .apply("FormatResults", MapElements
                        .into(TypeDescriptors.strings())
                        .via((KV<String, Long> wordCount) -> wordCount.getKey() + ": " + wordCount.getValue()))
                .apply("WriteCounts", TextIO.write().to(options.getOutputFile()));

        p.run().waitUntilFinish();

    }
}
