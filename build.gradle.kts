import com.google.protobuf.gradle.*

plugins {
    kotlin("jvm") version "1.6.10"
    id("com.google.protobuf") version "0.8.18"
    application
}

repositories {
    mavenCentral()
}

val grpcVersion = "1.45.1"
val grpcKotlinVersion = "1.2.1"
val protobufVersion = "3.20.0"

dependencies {
    implementation("org.jetbrains.kotlin:kotlin-stdlib:1.6.20")
    implementation("io.grpc:grpc-protobuf:$grpcVersion")
    implementation("io.grpc:grpc-stub:$grpcVersion")
    implementation("io.grpc:grpc-api:$grpcVersion")
    implementation("io.grpc:grpc-kotlin-stub:$grpcKotlinVersion")
    implementation("com.google.protobuf:protobuf-java-util:$protobufVersion")
    implementation("com.google.protobuf:protobuf-kotlin:$protobufVersion")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.6.1")
    runtimeOnly("io.grpc:grpc-netty:$grpcVersion")
}


application {
    mainClass.set("ServerKt")
}

java {
    toolchain {
        languageVersion.set(JavaLanguageVersion.of(17))
    }
}

kotlin {
    jvmToolchain {
        (this as JavaToolchainSpec).languageVersion.set(JavaLanguageVersion.of(17))
    }
}

tasks.withType<org.jetbrains.kotlin.gradle.tasks.KotlinCompile>().configureEach {
    kotlinOptions.freeCompilerArgs += "-opt-in=kotlin.RequiresOptIn"
}

protobuf {
    protoc {
        artifact = "com.google.protobuf:protoc:$protobufVersion"
    }
    plugins {
        id("grpc") {
            artifact = "io.grpc:protoc-gen-grpc-java:$grpcVersion"
        }
        id("grpckt") {
            artifact = "io.grpc:protoc-gen-grpc-kotlin:$grpcKotlinVersion:jdk7@jar"
        }
    }
    generateProtoTasks {
        all().forEach {
            it.plugins {
                id("grpc")
                id("grpckt")
            }
            it.builtins {
                id("kotlin")
            }
        }
    }
}