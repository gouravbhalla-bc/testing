import jetbrains.buildServer.configs.kotlin.v2019_2.*
import jetbrains.buildServer.configs.kotlin.v2019_2.buildSteps.script
import jetbrains.buildServer.configs.kotlin.v2019_2.triggers.vcs
import java.io.BufferedReader
import java.io.File

/*
The settings script is an entry point for defining a TeamCity
project hierarchy. The script should contain a single call to the
project() function with a Project instance or an init function as
an argument.

VcsRoots, BuildTypes, Templates, and subprojects can be
registered inside the project using the vcsRoot(), buildType(),
template(), and subProject() methods respectively.

To debug settings scripts in command-line, run the

    mvnDebug org.jetbrains.teamcity:teamcity-configs-maven-plugin:generate

command and attach your debugger to the port 8000.

To debug in IntelliJ Idea, open the 'Maven Projects' tool window (View
-> Tool Windows -> Maven Projects), find the generate task node
(Plugins -> teamcity-configs -> teamcity-configs:generate), the
'Debug' option is available in the context menu for the task.
*/

version = "2021.2"

var projectName = "AltonomyAceServices"

project {

    buildType(makeBuild("${projectName}_Build", "Build"))

    params {
        param("env.DOCKER_FILE_PATH", "Dockerfile")
        param("env.DOCKER_IMAGE_NAME", "altonomy/ace-services")
        param("env.PACKAGE_NAME", "altonomy-ace-services")
        param("env.HELM_CHART_PATH", "helm")
    }
}

fun loadScriptFromFile(path: String): String {
    val bufferedReader: BufferedReader = File(path).bufferedReader()
    return bufferedReader.use { it.readText() }.trimIndent()
}

fun makeBuild(
    buildId: String,
    buildName: String,
) = object : BuildType({
    id(buildId)
    name = buildName

    vcs {
        root(DslContext.settingsRoot)
    }

    steps {
        // set env vars for later steps
        script {
            name = "Set Env Vars"
            executionMode = BuildStep.ExecutionMode.DEFAULT
            scriptContent = loadScriptFromFile("scripts/set-env.sh")
        }

        // setup env
        script {
            name = "PYPI Setup"
            executionMode = BuildStep.ExecutionMode.DEFAULT
            scriptContent = loadScriptFromFile("scripts/pypi-setup.sh")
        }
        // script {
        //     name = "NPM Setup"
        //     executionMode = BuildStep.ExecutionMode.DEFAULT
        //     scriptContent = loadScriptFromFile("scripts/npm-setup.sh")
        // }

        // code quality
        script {
            name = "Lint"
            executionMode = BuildStep.ExecutionMode.DEFAULT
            scriptContent = loadScriptFromFile("scripts/common-lint.sh")
        }
        script {
            name = "Test"
            executionMode = BuildStep.ExecutionMode.DEFAULT
            scriptContent = loadScriptFromFile("scripts/common-test.sh")
        }

        // pypi
        script {
            name = "PYPI Package"
            executionMode = BuildStep.ExecutionMode.DEFAULT
            scriptContent = loadScriptFromFile("scripts/pypi-package.sh")
        }
        // script {
        //     name = "PYPI Package Docs"
        //     executionMode = BuildStep.ExecutionMode.DEFAULT
        //     scriptContent = loadScriptFromFile("scripts/pypi-docs.sh")
        // }

        // npm
        // script {
        //     name = "NPM Package"
        //     executionMode = BuildStep.ExecutionMode.DEFAULT
        //     scriptContent = loadScriptFromFile("scripts/npm-package.sh")
        // }
        // script {
        //     name = "NPM Package Docs"
        //     executionMode = BuildStep.ExecutionMode.DEFAULT
        //     scriptContent = loadScriptFromFile("scripts/npm-docs.sh")
        // }
        // script {
        //     name = "NPM Cleanup"
        //     executionMode = BuildStep.ExecutionMode.ALWAYS
        //     scriptContent = loadScriptFromFile("scripts/npm-cleanup.sh")
        // }

        // docker
        script {
            name = "Docker Package"
            executionMode = BuildStep.ExecutionMode.DEFAULT
            scriptContent = loadScriptFromFile("scripts/docker-package.sh")
        }
        script {
            name = "Docker Cleanup"
            executionMode = BuildStep.ExecutionMode.ALWAYS
            scriptContent = loadScriptFromFile("scripts/docker-cleanup.sh")
        }

        // helm
        script {
            name = "Helm Test"
            executionMode = BuildStep.ExecutionMode.DEFAULT
            scriptContent = loadScriptFromFile("scripts/helm-test.sh")
        }
        script {
            name = "Helm Package"
            executionMode = BuildStep.ExecutionMode.DEFAULT
            scriptContent = loadScriptFromFile("scripts/helm-package.sh")
        }

        // deploy
        script {
            name = "Deploy"
            executionMode = BuildStep.ExecutionMode.DEFAULT
            scriptContent = loadScriptFromFile("scripts/common-deploy.sh")
        }
    }

    triggers {
        vcs {
        }
    }
}) {}
