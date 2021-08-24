plugins {
    kotlin("jvm") version "1.5.20"
    kotlin("plugin.serialization") version "1.5.20"
    kotlin("kapt") version "1.5.20"
}

repositories {
    mavenCentral()
    //google()
}

dependencies {
    implementation("commons-io:commons-io:2.8.0")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.4.0-M1")
    implementation("io.github.microutils:kotlin-logging:2.0.3")

    implementation("org.jetbrains.kotlinx:kotlinx-serialization-core:1.0.0")
    implementation("org.jetbrains.kotlinx:kotlinx-serialization-json:1.0.0")
    implementation("org.jetbrains.kotlinx:kotlinx-serialization-protobuf:1.0.0")

    val koin_version = "3.1.2"
    implementation("io.insert-koin:koin-core:$koin_version")
    testImplementation("io.insert-koin:koin-core:$koin_version")

    // JUnit dependencies
    testImplementation(platform("org.junit:junit-bom:5.7.1"))
    testImplementation("org.junit.jupiter:junit-jupiter-api:5.7.1")
    testImplementation("org.junit.jupiter:junit-jupiter:5.7.1")
    testImplementation("org.apache.commons:commons-math3:3.4")
    testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine")
}

tasks {
    compileKotlin {
        kotlinOptions.jvmTarget = "16"
    }
    compileTestKotlin {
        kotlinOptions.jvmTarget = "16"
    }
}


tasks.named<Test>("test") {
    useJUnitPlatform()
}
