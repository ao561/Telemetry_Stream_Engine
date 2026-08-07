import com.google.protobuf.gradle.id

plugins {
    kotlin("jvm") version "2.4.10"
    kotlin("plugin.serialization") version "2.4.10"
    id("com.google.protobuf") version "0.10.0"
    application
}

group = "com.telemetry.stream"
version = "0.1.0"

repositories {
    mavenCentral()
}

val protobufVersion = "4.35.1"
val grpcVersion = "1.83.1"
val grpcKotlinVersion = "1.5.0"
val coroutinesVersion = "1.11.0"
val ktorVersion = "3.5.2"

dependencies {
    // Protobuf runtime (protobuf-kotlin brings the Kotlin DSL builders for generated messages)
    implementation("com.google.protobuf:protobuf-kotlin:$protobufVersion")

    // gRPC: transport, generated-stub support, and the coroutine stub runtime
    implementation("io.grpc:grpc-protobuf:$grpcVersion")
    implementation("io.grpc:grpc-stub:$grpcVersion")
    implementation("io.grpc:grpc-netty-shaded:$grpcVersion")
    implementation("io.grpc:grpc-services:$grpcVersion")
    implementation("io.grpc:grpc-kotlin-stub:$grpcKotlinVersion")

    // protoc-gen-grpc-java emits @javax.annotation.Generated, which is not on the JDK 21 classpath
    compileOnly("org.apache.tomcat:annotations-api:6.0.53")

    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:$coroutinesVersion")

    // Ktor: HTTP + WebSocket front end for the visualiser
    implementation("io.ktor:ktor-server-core:$ktorVersion")
    implementation("io.ktor:ktor-server-netty:$ktorVersion")
    implementation("io.ktor:ktor-server-websockets:$ktorVersion")
    // The browser calls POST /api/outage cross-origin from the Vite dev server.
    implementation("io.ktor:ktor-server-cors:$ktorVersion")
    implementation("io.ktor:ktor-server-content-negotiation:$ktorVersion")
    implementation("io.ktor:ktor-serialization-kotlinx-json:$ktorVersion")

    implementation("ch.qos.logback:logback-classic:1.5.38")

    testImplementation(kotlin("test"))
    testImplementation("org.jetbrains.kotlinx:kotlinx-coroutines-test:$coroutinesVersion")
    testImplementation("io.grpc:grpc-testing:$grpcVersion")
    testImplementation("io.grpc:grpc-inprocess:$grpcVersion")
}

kotlin {
    jvmToolchain(21)
}

protobuf {
    protoc {
        artifact = "com.google.protobuf:protoc:$protobufVersion"
    }
    plugins {
        // grpc-java: service base classes the grpc-kotlin stubs are layered on
        id("grpc") {
            artifact = "io.grpc:protoc-gen-grpc-java:$grpcVersion"
        }
        // grpc-kotlin: coroutine/Flow service stubs
        id("grpckt") {
            artifact = "io.grpc:protoc-gen-grpc-kotlin:$grpcKotlinVersion:jdk8@jar"
        }
    }
    generateProtoTasks {
        // Applies to every source set, so anything dropped into src/main/proto is
        // compiled to Java messages + Kotlin DSL builders + both stub flavours.
        all().forEach { task ->
            task.plugins {
                id("grpc")
                id("grpckt")
            }
            task.builtins {
                id("kotlin")
            }
        }
    }
}

application {
    mainClass.set("com.telemetry.stream.MainKt")
}

// Load generator: streams synthetic cluster telemetry at a running server.
tasks.register<JavaExec>("mockCluster") {
    group = "application"
    description = "Streams a simulated microservice cluster at localhost:50051."
    mainClass.set("com.telemetry.stream.tools.MockClusterGeneratorKt")
    classpath = sourceSets["main"].runtimeClasspath
}

// protobuf-gradle-plugin writes its protoc trampoline scripts into build/scripts, which is
// also the application plugin's default startScripts output dir - and that whole directory is
// copied into the distribution's bin/. Relocate ours so the dist ships only its own launchers.
tasks.startScripts {
    outputDir = layout.buildDirectory.dir("startScripts").get().asFile
}

tasks.test {
    useJUnitPlatform()
}
