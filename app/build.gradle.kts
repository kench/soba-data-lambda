plugins {
    java
    // Coverage, formatting and lint (Google Java Style)
    jacoco
    checkstyle
    id("com.diffplug.spotless") version "8.12.0"
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

val mockitoAgent = configurations.create("mockitoAgent")

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
    // Stripe
    implementation("com.stripe:stripe-java:31.4.1")
    // Data
    implementation("org.seattleoba:soba-dataformats:1.2")
    // Jackson for report generation
    implementation("com.fasterxml.jackson.core:jackson-annotations:2.20")
    implementation("com.fasterxml.jackson.dataformat:jackson-dataformat-csv:2.20.1")
    // JUnit
    testImplementation(platform(libs.junit.bom))
    testImplementation(libs.junit.jupiter)
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
    // Mockito, loaded as a Java agent by the test task
    testImplementation(libs.mockito.core)
    testImplementation(libs.mockito.junit.jupiter)
    mockitoAgent(libs.mockito.core) { isTransitive = false }
    // DynamoDB Local for unit testing
    testImplementation("software.amazon.dynamodb:DynamoDBLocal:3.2.0")
}

tasks.named<Test>("test") {
    // Use JUnit Platform for unit tests.
    useJUnitPlatform()
    // Load Mockito as an agent instead of self-attaching (required on JDK 21+).
    jvmArgs("-javaagent:${mockitoAgent.asPath}")
    // The Secrets Manager extension requests read the Lambda session token.
    environment("AWS_SESSION_TOKEN", "test-session-token")
    finalizedBy(tasks.jacocoTestReport)
}

jacoco {
    toolVersion = libs.versions.jacoco.get()
}

// Dagger-generated classes are excluded from coverage.
val coverageExcludes = listOf("**/Dagger*", "**/*_Factory*", "**/*_Provide*Factory*", "**/*_MembersInjector*")

tasks.jacocoTestReport {
    dependsOn(tasks.test)
    classDirectories.setFrom(files(classDirectories.files.map { fileTree(it) { exclude(coverageExcludes) } }))
    reports {
        xml.required = true
        html.required = true
    }
}

// Build fails if line coverage drops below 90%.
tasks.jacocoTestCoverageVerification {
    dependsOn(tasks.test)
    classDirectories.setFrom(files(classDirectories.files.map { fileTree(it) { exclude(coverageExcludes) } }))
    violationRules {
        rule {
            limit {
                counter = "LINE"
                value = "COVEREDRATIO"
                minimum = "0.90".toBigDecimal()
            }
        }
    }
}

tasks.check {
    dependsOn(tasks.jacocoTestCoverageVerification)
}

// google-java-format, checked by spotlessCheck (part of `check`). Run `./gradlew spotlessApply` to fix.
spotless {
    java {
        googleJavaFormat(libs.versions.google.java.format.get()).reflowLongStrings()
        removeUnusedImports()
        trimTrailingWhitespace()
        endWithNewline()
    }
}

// Checkstyle with Google's google_checks.xml (bundled with Checkstyle). Any warning fails the build.
checkstyle {
    toolVersion = libs.versions.checkstyle.get()
    config = resources.text.fromString(
        resources.text.fromArchiveEntry(
            configurations.checkstyle.get().filter { it.name.startsWith("checkstyle-") },
            "google_checks.xml"
        ).asString()
    )
    maxWarnings = 0
    isIgnoreFailures = false
}

// Apply a specific Java toolchain to ease working on different environments.
java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(25)
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