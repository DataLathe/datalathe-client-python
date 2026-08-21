import { dag, Directory, Workspace, check, func, object } from "@dagger.io/dagger";

const CLIENT_EXCLUDES = [".venv", "dist", ".git", ".github", "__pycache__", ".pytest_cache", "dagger"];

@object()
export class DatalatheClientPython {
  private clientSource?: Directory;

  constructor(ws?: Workspace) {
    this.clientSource = ws?.directory("/", { exclude: CLIENT_EXCLUDES });
  }

  /**
   * Workspace check: the full chip-lifecycle integration suite against an
   * ephemeral engine + chip-manager built from the pinned datalathe-backend.
   */
  @func()
  @check()
  async integration(): Promise<void> {
    if (!this.clientSource) {
      throw new Error("no workspace available");
    }
    await dag.datalathe().integrationPython({ pythonClient: this.clientSource });
  }
}
