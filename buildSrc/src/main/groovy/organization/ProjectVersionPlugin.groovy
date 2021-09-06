package organization

import org.eclipse.jgit.api.Git
import org.eclipse.jgit.api.errors.GitAPIException
import org.eclipse.jgit.storage.file.FileRepositoryBuilder
import org.gradle.api.Plugin
import org.gradle.api.Project
import org.gradle.api.logging.Logger
import org.gradle.api.logging.Logging

/**
 * Assigns version to all projects.
 */
class ProjectVersionPlugin implements Plugin<Project> {

  private static final Logger LOGGER = Logging.getLogger(ProjectVersionPlugin)

  private static String readVersion(Project project) {
    String version = 'UNKNOWN'
    try {
      def repositoryBuilder = new FileRepositoryBuilder()
          .readEnvironment()
          .findGitDir(project.projectDir)
      if (repositoryBuilder.gitDir != null) {
        new Git(repositoryBuilder.build()).withCloseable { git ->
          def status = git.status().call()
          version = git.describe().call()
          if (!status.clean || version ==~ /.*-\d+-g[0-9a-f]+/) {
            // Assign version for work in progress.
            version = '999-SNAPSHOT'
          }
        }
      }
    } catch (GitAPIException | IOException e) {
      LOGGER.error 'Failed to read version', e
    }

    LOGGER.quiet "Version ${version}"
    return version
  }

  @Override
  void apply(Project project) {
    String version = readVersion(project)
    project.allprojects { subProject ->
      subProject.version = version
    }
  }
}
