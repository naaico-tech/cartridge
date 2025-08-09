## Contributing to Cartridge

Thank you for your interest in contributing! We welcome issues and pull requests from the community.

### How to Contribute

1. Fork the repository to your own GitHub account
2. Create a new branch from `main` for your change
   - Use a descriptive name: `feature/...`, `fix/...`, or `docs/...`
3. If your change relates to a bug or feature, open an issue first and reference it in your PR
4. Make your changes following our code style and add tests where appropriate
5. Run tests locally and ensure they pass
6. Submit a pull request to `main` with a clear description and screenshots/logs if relevant

### Development Setup

- The service code is in `cartridge-init/`
- Use the provided `docker-compose.yml` for local services
- Python version: 3.9+

### Testing

From `cartridge-init/`:

```bash
pip install -e ".[dev,test]"
pytest
```

### Commit Messages

Follow Conventional Commits when possible:

- feat: add new feature
- fix: fix a bug
- docs: update documentation
- chore: maintenance tasks
- refactor: code change that neither fixes a bug nor adds a feature

### Pull Request Guidelines

- Reference related issue(s) using `Closes #123` or `Refs #123`
- Include tests for new functionality
- Update documentation (README, examples) when behavior changes
- Ensure CI checks are green

### Code of Conduct

Be respectful and inclusive. Harassment and abuse are not tolerated.

### License

By contributing, you agree that your contributions will be licensed under the repository’s license.


