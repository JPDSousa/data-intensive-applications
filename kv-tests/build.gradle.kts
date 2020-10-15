plugins {
    kotlin("jvm")
}

repositories {
    mavenCentral()
}

dependencies {
    implementation(kotlin("stdlib-jdk8"))
    implementation(project(":kv-api"))
    implementation(platform("org.junit:junit-bom:5.7.0"))
    implementation("org.junit.jupiter:junit-jupiter")
    implementation("commons-io:commons-io:2.8.0")
    implementation("org.apache.commons:commons-math3:3.4")
}

tasks {
    compileKotlin {
        kotlinOptions.jvmTarget = "1.8"
    }
    compileTestKotlin {
        kotlinOptions.jvmTarget = "1.8"
    }
}
