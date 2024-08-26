package io.amrk7.flarebox;

import static io.amrk7.flarebox.SparkSubmitClient.Version.SPARK_33;

public class Example {
    public static void main(String[] args) {
        String[] paths = new String[2];
        paths[0] = "file:/Users/amruths/Uber/java_micros/spark-chamber/core/target/spark-core_2.12-3.3.2-uber-18.jar";
        paths[1] = "file:/Users/amruths/Uber/java_micros/spark-chamber/launcher/target/spark-launcher_2.12-3.3.2-uber-18.jar";
        SPARK_33.initialize(paths);
        SparkSubmitClient.sparkSubmit(SPARK_33, args);
    }
}
