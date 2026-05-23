from textwrap import dedent

import pytest

from engine.parser import PipelineValidationError, parse_pipeline_text


def test_parse_pipeline_text_returns_typed_pipeline_definition():
    pipeline = parse_pipeline_text(
        dedent(
            """
            name: build-lib-http
            version: 1.0.0
            dependencies:
              - name: lib-core
                version: ^1.0.0
            jobs:
              build:
                runtime: alpine:3.18
                resources:
                  cpu: 1.0
                  memory: 512Mi
                steps:
                  - name: test
                    run: sh ./test.sh
            artifacts:
              - name: lib-http
                version: 1.0.0
                path: ./out.tar.gz
            """
        )
    )

    assert pipeline.name == "build-lib-http"
    assert pipeline.jobs["build"].runtime == "alpine:3.18"
    assert pipeline.dependencies[0].name == "lib-core"


def test_job_definition_can_render_runner_script_from_steps():
    pipeline = parse_pipeline_text(
        dedent(
            """
            name: build-lib-http
            version: 1.0.0
            jobs:
              build:
                runtime: alpine:3.18
                resources:
                  cpu: 1.0
                  memory: 512Mi
                steps:
                  - name: test
                    run: sh ./test.sh
                  - name: package
                    run: tar czf out.tar.gz src/
            artifacts:
              - name: lib-http
                version: 1.0.0
                path: ./out.tar.gz
            """
        )
    )

    script = pipeline.jobs["build"].to_shell_script()

    assert "set -e" in script
    assert "sh ./test.sh" in script
    assert "tar czf out.tar.gz src/" in script


def test_parse_pipeline_text_rejects_unknown_top_level_field():
    with pytest.raises(PipelineValidationError) as exc_info:
        parse_pipeline_text(
            dedent(
                """
                name: build-lib-http
                version: 1.0.0
                unexpected: true
                jobs: {}
                artifacts: []
                """
            )
        )

    error = exc_info.value
    assert "unexpected" in str(error)
    assert error.line == 4


def test_parse_pipeline_text_rejects_missing_required_job_steps():
    with pytest.raises(PipelineValidationError) as exc_info:
        parse_pipeline_text(
            dedent(
                """
                name: build-lib-http
                version: 1.0.0
                jobs:
                  build:
                    runtime: alpine:3.18
                    resources:
                      cpu: 1.0
                      memory: 512Mi
                artifacts:
                  - name: lib-http
                    version: 1.0.0
                    path: ./out.tar.gz
                """
            )
        )

    error = exc_info.value
    assert "steps" in str(error)
    assert error.path == "jobs.build.steps"


def test_parse_pipeline_text_rejects_duplicate_artifact_coordinates():
    with pytest.raises(PipelineValidationError) as exc_info:
        parse_pipeline_text(
            dedent(
                """
                name: build-lib-http
                version: 1.0.0
                jobs:
                  build:
                    runtime: alpine:3.18
                    resources:
                      cpu: 1.0
                      memory: 512Mi
                    steps:
                      - name: package
                        run: tar czf out.tar.gz src/
                artifacts:
                  - name: lib-http
                    version: 1.0.0
                    path: ./out.tar.gz
                  - name: lib-http
                    version: 1.0.0
                    path: ./other.tar.gz
                """
            )
        )

    assert "duplicate artifact" in str(exc_info.value).lower()
