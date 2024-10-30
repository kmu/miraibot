import json
import os
import signal
import socket
from collections import defaultdict
from io import StringIO
import datetime

import pandas as pd
import paramiko
import requests
from paramiko_expect import SSHClientInteraction
from slack_sdk.web import WebClient


class TimeoutException(Exception):
    def __init__(self, seconds, msg=""):
        self.timeout_limit = seconds
        if msg != "":
            msg = ": " + msg
        super().__init__(f"Timeout {seconds} sec" + msg)


class TimeoutContext:
    def __init__(self, seconds, err_msg=""):
        self.seconds = seconds
        self.err_msg = err_msg

    def handler(self, signum, frame):
        raise TimeoutException(self.seconds, self.err_msg)

    def __enter__(self):
        signal.signal(signal.SIGALRM, self.handler)
        signal.alarm(self.seconds)

    def __exit__(self, exc_type, exc_value, traceback):
        signal.alarm(0)
        signal.signal(signal.SIGALRM, signal.SIG_DFL)


PROMPT = "(\([\-0-9A-z_]+\)\s)?~\s>\s"  # noqa: W605


user = os.environ["SSH_USER"]

host = os.environ["SSH_GATEWAY_HOST"]
machine = os.environ["SSH_MACHINE"]


def post_lab_slack(
    text: str, username="mirai", emoji: str = ":ssh-mirai:", ts=None
) -> None:
    web_client = WebClient(token=os.environ["LAB_TOKEN"])
    return web_client.chat_postMessage(
        text=text,
        channel=os.environ["LAB_CHANNEL"],
        username=username,
        icon_emoji=emoji,
        thread_ts=ts,
    )


def post_slack(text: str) -> None:
    WEB_HOOK_URL = os.environ["WEB_HOOK_URL"]
    requests.post(
        WEB_HOOK_URL,
        data=json.dumps(
            {
                "text": text,
                "username": f"stat bot ({socket.gethostname()})",
                "link_names": 1,
            }
        ),
    )


def get_interaction():
    proxy = paramiko.ProxyCommand(f"ssh {user}@{host} -p 22 nc {machine} 22")
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(  # lgtm [py/paramiko-missing-host-key-validation]
        paramiko.AutoAddPolicy
    )
    client.connect(machine, username=user, sock=proxy)

    def output(x):
        return None

    return SSHClientInteraction(
        client, timeout=10, display=True, output_callback=output, tty_width=250
    )


def get_output(command: str) -> None:
    with get_interaction() as interact:
        interact.send("")
        interact.expect(PROMPT)

        interact.send(command)
        interact.expect(PROMPT)

        output = interact.current_output
        output = "\n".join(output.split("\n")[1:-1])
        return output


def lab_update(ts=None):
    usage = get_output("/usr/sge/bin/linux-x64/qstat -f")
    # usage = f"```\n{usage}\n```"
    post_lab_slack(usage, ts=ts)

    mirai = get_output("/usr/sge/bin/linux-x64/qstat")
    mirai = f"```\n{mirai}\n```"
    mirai_last = ""
    if os.path.exists("mirai.txt"):
        with open("mirai.txt") as f:
            mirai_last = f.read()

    with open("mirai.txt", "w") as f:
        f.write(mirai)

    if mirai != mirai_last:
        # post_slack(mirai)
        post_lab_slack(mirai, ts=ts)


def pretty_lab_update():
    qstat = get_output("qstat -f")
    qstat = qstat.split("\n\n########")[0]
    qstat = qstat.replace("linux-x64     a", "linux-x64")
    qstat += "\n"

    reserved_d = defaultdict(list)
    actual_d = defaultdict(list)
    jobtime_d = defaultdict(list)

    for node in qstat.split(
        "---------------------------------------------------------------------------------\n"
    ):

        if ".q@compute-" in node:
            line_elements = node.split("\n")[0].split()
            queue = line_elements[0]
            resv_used_tot = line_elements[2]
            _load_avg = line_elements[3]

            if _load_avg != "-NA-":
                load_avg = float(_load_avg)

            q_group = queue.split("@")[0]

            _, _, _equipped_cpus = resv_used_tot.split("/")
            equipped_cpus = float(_equipped_cpus)

            reserved_emoji = ":ジョブなし:"

            if len(node.split("\n")) > 2:

                user_d = defaultdict(int)

                for user_line in node.split("\n")[1:-1]:
                    user = user_line.split()[3]
                    user_resv = user_line.split()[-1]

                    user_d[user] += int(user_resv)

                if len(user_d.keys()) == 1:
                    if list(user_d.values())[0] == equipped_cpus:
                        reserved_emoji = f":{user}:"
                    else:
                        reserved_emoji = ":余裕:"
                else:
                    if sum(list(user_d.values())) == equipped_cpus:
                        reserved_emoji = ":全力:"
                    else:
                        reserved_emoji = ":余裕:"

            if line_elements[-1] == "d":
                actual_emoji = ":disconnected:"
            elif _load_avg == "-NA-":
                actual_emoji = ":disconnected:"
            elif load_avg > float(equipped_cpus) + 0.5:
                actual_emoji = ":cpu利用率超過:"
            elif load_avg > float(equipped_cpus) - 1.0:
                actual_emoji = ":全力:"
            elif load_avg < 1.0:
                actual_emoji = ":ジョブなし:"
            elif load_avg < 32:
                actual_emoji = f":n{int(load_avg)}:"
            else:
                actual_emoji = ":余裕:"

            reserved_d[q_group] += [reserved_emoji]
            actual_d[q_group] += [actual_emoji]

            time_emoji = ":ジョブなし:"

            if len(node.split("\n")) > 2:
                JST = datetime.timezone(datetime.timedelta(hours=+9), "JST")
                nowtime = datetime.datetime.now(JST)
                latest_jobtime = datetime.datetime(2000, 1, 1, 0, 0, 0, 0, tzinfo=JST)
                for user_line in node.split("\n")[1:-1]:
                    jobtime_str = user_line.split()[5] + " " + user_line.split()[6]
                    jobtime = datetime.datetime.strptime(
                        jobtime_str, "%m/%d/%Y %H:%M:%S"
                    )
                    jobtime = jobtime.replace(tzinfo=JST)
                    if latest_jobtime < jobtime:
                        latest_jobtime = jobtime

                total_jobtime = (nowtime - latest_jobtime).total_seconds()
                if total_jobtime < 4 * 60:  # under 1h
                    time_emoji = f":{int(total_jobtime/60)}m:"
                elif total_jobtime < 60 * 60:  # under 1h
                    time_emoji = f":{int(total_jobtime/60/15)*15}m:"
                elif total_jobtime < 4 * 60 * 60:  # under 4h
                    time_emoji = f":{int(total_jobtime/60/60)}h:"
                elif total_jobtime < 24 * 60 * 60:  # under 1d
                    time_emoji = f":{int(total_jobtime/60/60/4)*4}h:"
                elif total_jobtime < 14 * 24 * 60 * 60:  # under 2week
                    time_emoji = f":{int(total_jobtime/60/60/24)}d:"
                else:
                    time_emoji = ":over14d:"

            jobtime_d[q_group] += [time_emoji]

    msg = ""
    for group, reserved in reserved_d.items():
        msg += f"*{group}*\n"
        msg += " ".join(reserved) + " reserved\n"
        msg += " ".join(actual_d[group]) + " actual\n"
        msg += " ".join(jobtime_d[group]) + " time\n"

    return post_lab_slack(msg)


def my_update():
    cmd = ["/usr/sge/bin/linux-x64/qstat", "-u", user, "|", "grep", "-v", "compute-3-1"]
    cmd = " ".join(cmd)

    my_mirai = get_output(cmd)

    my_mirai = "`mirai updates:`\n```\n" + my_mirai + "```\n"

    mirai_last = ""
    if os.path.exists("my_mirai.txt"):
        with open("my_mirai.txt") as f:
            mirai_last = f.read()

    with open("my_mirai.txt", "w") as f:
        f.write(my_mirai)

    if my_mirai != mirai_last:
        post_slack(my_mirai)


def memory_usage():
    qhost = get_output("/usr/sge/bin/linux-x64/qhost")
    # post_lab_slack(f"```\n{qhost}\n```\n")
    df = pd.read_csv(
        StringIO(qhost),
        skiprows=3,
        sep="\s+",  # noqa: W605
        names=[
            "node",
            "os",
            "cores",
            "load",
            "max_mem",
            "used_mem",
            "max_swap",
            "used_swap",
        ],
    )

    for mem in "max_mem", "used_mem", "max_swap", "used_swap":
        df.loc[:, mem] = (
            df[mem]
            .astype(str)
            .str.replace("-", "0")
            .str.replace("K", "e3")
            .str.replace("M", "e6")
            .str.replace("G", "e9")
            .astype(float)
        )

    df["MEMUSE"] = df.used_mem / (df.max_mem+1e-10) * 100

    high_memory_ratio = 95
    high_memory = df[df["MEMUSE"] > high_memory_ratio]

    qstat = get_output("/usr/sge/bin/linux-x64/qstat | tail -n +3")

    df_qstat = pd.read_csv(
        StringIO(qstat),
        sep="\s+",  # noqa: W605
        names=[
            "jobID",
            "prior",
            "name",
            "user",
            "state",
            "date",
            "time",
            "queue",
            "slots",
        ],
    )

    df_qstat["node"] = df_qstat.queue.str.split("@").str[1]

    merged_df = high_memory.merge(df_qstat, how="inner")

    if len(merged_df) > 0:
        msg = ""
        for user in merged_df["user"].unique():
            # userごとにメモリ使用量が高いキューをまとめる
            queues = ", ".join(merged_df[merged_df["user"] == user]["queue"].unique())

            msg += f"@{user}\n:warning: {queues}のジョブが"
            msg += f"{high_memory_ratio}%以上のメモリを消費してしまっています。"
            msg += "低速化やクラッシュの恐れがあります。\n"
            msg += "よりメモリの大きなノードを使用しましょう。\n"

        # post_slack(msg)
        post_lab_slack(msg)

    df.load = df.load.replace("-", "0").str.replace("K", "e3").replace("", "0").astype(float)
    df.cores = df.cores.replace("-", "0").astype(float)
    df["free_cpus"] = df.load - df.cores
    df_overcpu = df[df.free_cpus > 1]

    df_overcpu = df_overcpu.merge(df_qstat, how="inner")

    if len(df_overcpu) > 0:
        msg = ""
        for user in df_overcpu["user"].unique():
            # userごとに割り当てコア数以上のCPUを利用しているキューをまとめる
            queues = ", ".join(df_overcpu[df_overcpu["user"] == user]["queue"].unique())

            msg += f"@{user}\n:warning: {queues}のジョブが"
            msg += "割り当てコア数以上のCPUを消費しています。"
            msg += "並列化の問題か、ゾンビプロセスの存在の可能性があります。\n"

        # post_slack(msg)
        post_lab_slack(msg)

def check_error():
    msg = ""

    Eqw_errors = get_output("qstat -f | grep Eqw").split("\n")
    if Eqw_errors != ['']:
        for Eqw_error in Eqw_errors:
            job_id = Eqw_error.split()[0]
            user_name = Eqw_error.split()[3]
            error_reason_str =  get_output("qstat -f | grep Eqw")
            msg += f"@{user_name}\n:warning: {job_id}のジョブに問題が発生している可能性があります。\n"
            msg += error_reason_str + "\n"
    if msg != "":
        post_lab_slack(msg)


def main():
    memory_usage()
    check_error()
    res = pretty_lab_update()
    lab_update(ts=res.get("ts", None))


if __name__ == "__main__":
    main()
