#!/usr/bin/env python
import rospy
import signal
import os
import rosbag
import subprocess
from rospy import Time


def get_earliest_rosbag_timestamp():
    """
    Scans all .bag files in a folder and returns the earliest timestamp
    as a rospy.Time object.
    """
    earliest_time = None
    folder_path = "/rustle/dataset"

    for file_name in os.listdir(folder_path):
        if file_name.endswith('.bag'):
            bag_path = os.path.join(folder_path, file_name)
            try:
                with rosbag.Bag(bag_path, 'r') as bag:
                    start_time = bag.get_start_time()
                    if earliest_time is None or start_time < earliest_time:
                        earliest_time = start_time
            except Exception as e:
                rospy.logwarn(f"Failed to read {bag_path}: {e}")

    if earliest_time is None:
        raise ValueError("No valid bag files found or all were unreadable.")

    # Convert to rospy.Time
    secs = int(earliest_time)
    nsecs = int((earliest_time - secs) * 1e9)
    return rospy.Time(secs, nsecs)


class CutManager:
    def __init__(self):
        self.relay_processes = {}
        self.node_start_time = get_earliest_rosbag_timestamp()
        
        # Load parameters
        self.cut_config = rospy.get_param("cut_list")
        self.update_interval = rospy.Duration(0.1)  # Check every 0.1 second
        
        # Start periodic update
        self.timer = rospy.Timer(self.update_interval, self.update_nodes)
        rospy.loginfo("Cut manager started")

    def is_active(self, active_periods):
        """Check if current ROS time is within any active period"""
        current_time = (rospy.get_rostime() - self.node_start_time).to_sec()

        if len(active_periods) == 0 :
            return True
        
        for period in active_periods:
            start = period["start_time"]
            end = start + period["duration"]
            if start <= current_time <= end:
                return True
        return False


    def start_relay_node(self, sensor, topic):
        """Start a relay node when cuts are inactive"""
        if sensor in self.relay_processes:
            return
            
        args = ["rosrun", "topic_tools", "relay", topic, f"{topic}_cut"]

        self.relay_processes[sensor] = subprocess.Popen(
            args,
            preexec_fn=os.setsid
        )

        rospy.loginfo(f"Started relay for {sensor}")

    def stop_node(self, process_dict, sensor):
        """Stop a running node and its process group"""
        if sensor in process_dict:
            process = process_dict[sensor]
            try:
                os.killpg(os.getpgid(process.pid), signal.SIGTERM)
                process.wait()
                rospy.loginfo(f"Stopped {sensor} node")
            except Exception as e:
                rospy.logwarn(f"Error killing {sensor} process: {e}")
            del process_dict[sensor]
            rospy.loginfo(f"Stopped {sensor} node")

    def update_nodes(self, event):
        """Periodic update check"""
        for config in self.cut_config:
            sensor = config["sensor"]
            topic = config["topic"]
            active_periods = config["active_periods"]

            if self.is_active(active_periods):
                self.stop_node(self.relay_processes, sensor)
            else:
                self.start_relay_node(sensor, topic)

    def shutdown(self):
        """Cleanup on shutdown"""
        for sensor in list(self.relay_processes.keys()):
            self.stop_node(self.relay_processes, sensor)

if __name__ == "__main__":
    rospy.init_node("dynamic_cut_manager")
    manager = CutManager()
    rospy.on_shutdown(manager.shutdown)
    rospy.spin()