#FLINK BOSS Tutorial

Welcome to the Flink tutorial at the BOSS workshop of VLDB 2015.

## Goals

The workshop gives a brief hands-on introduction to Flink's batch, graph and stream processing API's.
For the batch and graph intro please go to `src/main/boss/gelly`, for streaming to `src/main/boss/streaming` respectively.

## Setup

In case of temporary WiFi outages we are distributing USB sticks with the code, the Flink documentation and the Eclipse IDE.
To setup the environment do the following:

1. Open Eclipse
2. Create a new Java project: *File -> New -> Java Project*, name it *flink-boss*.
3. Import data into project: *Right click on project -> Import -> General -> Filesystem*
4. Add Flink fat jar to build path: *Right click on project -> Build Path -> Configure build path -> Libraries -> Add JARs*. Add `flink-dist-*.jar` from the `lib/` folder.

Now you are set to run Flink! :)

As an alternative for seasoned developers with access to the internet we provide a git repository at `https://github.com/mbalassi/boss-tutorial.git` set up with maven. Clone the project and import it to your favorite IDE.
