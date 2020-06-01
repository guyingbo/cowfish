import re
import signal
import subprocess as sp
import sys
import time

import pytest
import requests

url_pattern = re.compile(r"http://127.0.0.1:(?P<port>\d+)")


def start_service(service_name):
    args = [
        sys.executable,
        "-m",
        "moto.server",
        "-p",
        "0",
        service_name,
    ]
    # If test fails stdout/stderr will be shown
    process = sp.Popen(args, stdin=sp.PIPE, stderr=sp.PIPE)

    for i in range(0, 30):
        if process.poll() is not None:
            _, errs = process.communicate()
            pytest.fail(
                f"service failed starting up: {service_name}\n{i}{errs.decode()}"
            )
            break

        line = process.stderr.readline().decode()
        url = url_pattern.search(line).group()

        try:
            requests.get(url, timeout=0.5)
            break
        except requests.exceptions.ConnectionError:
            time.sleep(0.5)
    else:
        stop_process(process)  # pytest.fail doesn't call stop_process
        pytest.fail(f"Can not start service: {service_name}")

    return url, process


def stop_process(process):
    try:
        process.send_signal(signal.SIGTERM)
        process.communicate(timeout=20)
    except sp.TimeoutExpired:
        process.kill()
        outs, errors = process.communicate(timeout=20)
        exit_code = process.returncode
        msg = "Child process finished {} not in clean way: {} {}".format(
            exit_code, outs, errors
        )
        raise RuntimeError(msg)


@pytest.yield_fixture(scope="session")
def s3_server():
    url, process = start_service("s3")

    try:
        yield url
    finally:
        stop_process(process)


@pytest.yield_fixture(scope="session")
def dynamodb2_server():
    url = process = start_service("dynamodb2")

    try:
        yield url
    finally:
        stop_process(process)


@pytest.yield_fixture(scope="session")
def cloudformation_server():
    url, process = start_service("cloudformation")

    try:
        yield url
    finally:
        stop_process(process)


@pytest.yield_fixture(scope="session")
def sns_server():
    url, process = start_service("sns")

    try:
        yield url
    finally:
        stop_process(process)


@pytest.yield_fixture(scope="function")
def sqs_server():
    url, process = start_service("sqs")

    def stop():
        stop_process(process)

    try:
        yield url, stop
    finally:
        stop_process(process)


@pytest.yield_fixture(scope="session")
def kinesis_server():
    url, process = start_service("kinesis")

    try:
        yield url
    finally:
        stop_process(process)
