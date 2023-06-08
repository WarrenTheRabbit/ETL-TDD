## Commit Guidelines

| Commit Type | Description | Example |
|-------------|-------------|---------|
| `feat` | Introduces a new feature to the codebase. | `feat: add new login button` |
| `fix` | Fixes a bug. | `fix: resolve issue with user login` |
| `docs` | Adds or updates documentation. | `docs: add explanation of login process` |
| `style` | Makes code style changes (white-space, formatting, missing semi-colons, etc). Does not affect functionality. | `style: reformat login module for readability` |
| `refactor` | Changes the codebase without fixing a bug or adding a feature. Typically restructuring the code. | `refactor: simplify login logic` |
| `perf` | Improves performance. | `perf: optimize login query` |
| `test` | Adds or modifies tests. | `test: add tests for login module` |
| `chore` | Changes to the build process or auxiliary tools and libraries such as documentation generation. | `chore: update build script` |
| `revert` | Reverts a previous commit. | `revert: revert commit a1b2c3d4` |
| `scope` | Optional, can be anything specifying place of the commit change. | `fix(server): fix server crash issue` |

**Note:** The `scope` is optional and can be anything specifying the place of the commit change. For example, `fix(server): fix server crash issue` indicates a bug fix in the server.

**Format:** The commit messages should follow this format: `<type>(<scope>): <description>`

- `<type>`: This represents the type of change being made. It should be one of the types listed in the table above.
- `<scope>`: This is optional. It can be anything specifying the place of the commit change.
- `<description>`: A brief description of the change.

**Breaking Changes:** If the change is a breaking change, it should be noted as `BREAKING CHANGE:` in the body or footer of the commit message
