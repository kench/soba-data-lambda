plugins {
    java
}

repositories {
    // Use Maven Central for resolving dependencies.
    mavenCentral()
    // Add GitHub Packages
    maven {
        url = uri("https://maven.pkg.github.com/kench/soba-dataformats")
        credentials {
            username = System.getenv("GITHUB_ACTOR")
            password = System.getenv("GITHUB_TOKEN")
        }
    }
}

dependencies {
    // This dependency is used by the application.
    implementation(libs.guava)
    // Log4j2
    implementation("org.apache.logging.log4j:log4j-api:2.25.3")
    implementation("org.apache.logging.log4j:log4j-core:2.25.3")
    // Dagger
    implementation("com.google.dagger:dagger:2.53.1")
    annotationProcessor("javax.annotation:javax.annotation-api:1.3.2")
    annotationProcessor("com.google.dagger:dagger-compiler:2.53.1")
    // AWS Lambda
    implementation("com.amazonaws:aws-lambda-java-core:1.4.0")
    implementation("com.amazonaws:aws-lambda-java-events:3.16.1")
    runtimeOnly("com.amazonaws:aws-lambda-java-log4j2:1.5.1")
    // AWS SDK
    // DynamoDB Java client
    implementation(platform("software.amazon.awssdk:bom:2.41.10"))
    implementation("software.amazon.awssdk:dynamodb-enhanced")
    implementation("software.amazon.awssdk:s3")
    implementation("software.amazon.awssdk:sqs")
    // twitch4j
    implementation("com.github.twitch4j:twitch4j:1.25.0")
    // Data
    implementation("org.seattleoba:soba-dataformats:1.0")
    // Jackson for report generation
    implementation("com.fasterxml.jackson.core:jackson-annotations:2.20")
    implementation("com.fasterxml.jackson.dataformat:jackson-dataformat-csv:2.20.1")
    // Mockito
    testImplementation("org.mockito:mockito-core:5.21.0")
    testImplementation("org.mockito:mockito-junit-jupiter:5.21.0")
    // DynamoDB Local for unit testing
    testImplementation("software.amazon.dynamodb:DynamoDBLocal:3.2.0")
}

testing {
    suites {
        // Configure the built-in test suite
        val test by getting(JvmTestSuite::class) {
            // Use JUnit Jupiter test framework
            useJUnitJupiter("5.10.1")
        }
    }
}

// Apply a specific Java toolchain to ease working on different environments.
java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(21)
    }
}

tasks {
    register<Zip>("buildZip") {
        dependsOn(jar)
        from(compileJava)
        from(processResources)
        into("lib") {
            from(configurations.runtimeClasspath)
        }
    }

    build {
        dependsOn("buildZip")
    }
}