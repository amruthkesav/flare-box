package io.amrk7.flarebox;

import java.net.URL;
import java.net.URLClassLoader;

public class SparkSubmitClient {

    public enum Version {
        SPARK_33, SPARK_24, SPARK_30;

        private URL[] jarPaths;
        private final String mainClass = "org.apache.spark.deploy.SparkSubmit";

        void initialize(String[] jarPathsStrings) {
            try {
                jarPaths = new URL[jarPathsStrings.length];
                for (int i = 0; i < jarPathsStrings.length; i++) {
                    jarPaths[i] = new URL(jarPathsStrings[i]);
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    private static class SparkSubmitCL extends URLClassLoader {
        public SparkSubmitCL(Version version, ClassLoader parent) {
            super(version.jarPaths, parent);
        }

        @Override
        protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
            return super.loadClass(name, resolve);
        }
    }

    public static void sparkSubmit(Version clientVersion, String[] args) {
        final ClassLoader currentLoader = Thread.currentThread().getContextClassLoader();
        try {
            final SparkSubmitCL sparkSubmitCL = new SparkSubmitCL(clientVersion, currentLoader);
            Thread.currentThread().setContextClassLoader(sparkSubmitCL);
            Class<?> sparkSubmitClass = sparkSubmitCL.loadClass(clientVersion.mainClass);
            sparkSubmitClass.getMethod("main", String[].class).invoke(null, (Object) args);
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            Thread.currentThread().setContextClassLoader(currentLoader);
        }
    }
}
