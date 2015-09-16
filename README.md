#FLINK BOSS Tutorial

Welcome to the Flink tutorial at the BOSS workshop of VLDB 2015.

## Goals

The workshop gives a brief hands-on introduction to Flink's batch, graph and stream processing API's.

## Setup

In case of temporary WiFi outages we are distributing USB sticks with the code, the Flink documentation and the Eclipse IDE.
To setup the environment do the following:

1. Open Eclipse
2. Create a new Java project: *File -> New -> Java Project*, name it *flink*.
3. Copy-paste the contents of the flink-boss folder (all of them!) into the location of the newly created java project. Make sure you "refresh" the jave project so that Eclipse finds the source files.
4. Open the lib folder in the Eclipse project and right click on the flink jar file, and add it to the build path (Build Path -> Add to Build Path)

Now you are set to run Flink! :)

For the batch and graph intro please go to `src/boss/gelly`, for streaming to `src/boss/streaming` respectively.

As an alternative for seasoned developers with access to the internet we provide a git repository at `https://github.com/mbalassi/boss-tutorial.git` set up with maven. Clone the project and import it to your favorite IDE.

## Slides

The slides used during the workshop are located in the `slides` folder.

## Code exercise solutions

The solution for the exercises can be found on the `solution` git branch.
