<!-- Thanks for contributing! Please fill in the checklist below. -->

## Summary

<!-- What does this PR change and why? -->

## Related issues

<!-- e.g. Closes #123 -->

## Checklist

- [ ] `mvn verify` passes locally
- [ ] Tests added/updated (JUnit 4) and pass without a running broker
- [ ] No Java 9+ APIs (Java 8 bytecode target)
- [ ] If an endpoint option was added: the matching `metadata-sender-*.xml` /
      `metadata-receiver-*.xml` file (both the `AttributeReference` and
      `AttributeMetadata` entries), getter/setter, and a
      `CpiKafkaPlusComponentTest` assertion were updated
- [ ] Documentation updated for any change in observable behaviour:
      `docs/configuration.md` (parameter rows, UI tab/group headings) and the
      affected page(s) under `docs/features/` — see
      [CONTRIBUTING.md → Updating the documentation](../CONTRIBUTING.md#updating-the-documentation)
- [ ] No secrets, credentials, tenant names or other internal data included
