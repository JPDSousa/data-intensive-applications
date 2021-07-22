plugins {
    kotlin("jvm") version "1.5.20"
    kotlin("plugin.serialization") version "1.5.20"
}

repositories {
    mavenCentral()
}

dependencies {
    implementation("commons-io:commons-io:2.8.0")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.4.0-M1")
    implementation("io.github.microutils:kotlin-logging:2.0.3")
    implementation("org.jetbrains.kotlinx:kotlinx-serialization-core:1.0.0")
    implementation("org.jetbrains.kotlinx:kotlinx-serialization-json:1.0.0")
    implementation("org.jetbrains.kotlinx:kotlinx-serialization-protobuf:1.0.0")
    testImplementation(platform("org.junit:junit-bom:5.7.0"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testImplementation("org.apache.commons:commons-math3:3.4")
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
