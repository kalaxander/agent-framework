# Camel integration — what's here and why

`route.yaml` is a real Apache Camel route definition in Camel's YAML DSL, showing how Camel
would sit in front of this framework's REST ingress to bridge an external protocol (a file-drop
folder, in this example) into a normal `POST /v1/runs` call.

**What's verified vs. illustrative:** the Python/agentframework side of every other Phase 7
piece (`integrations/airflow_adapter.py`) is actually executed and tested in this repo — see
`run_demo_phase7.py`. This route file is not, because Camel is a JVM framework and this sandbox
has no JVM or network access to install a Camel distribution. It's provided as a correctly-
structured, standard Camel config (not fabricated capability) rather than something claimed to
have been run here — see docs/Memory.md for this flagged as a deviation from "everything in this
repo has been executed."

To actually run it once you have Camel available, `camel-jbang` is the fastest path (no project
scaffolding needed):

```bash
jbang app install camel-jbang@apache/camel
camel run route.yaml
```
