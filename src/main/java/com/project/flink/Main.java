package com.project.flink;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Main {

    public static void main(String[] args) throws Exception {
        // Créer l'environnement d'exécution Flink
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
        // Appeler le job Flink
        FlinkJob.executeFlinkJob(env);
    }
}
