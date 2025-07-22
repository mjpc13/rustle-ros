#!/usr/bin/env python
import rospy
import signal
import os
import rosbag
import subprocess
from rostopic import get_topic_class
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
                rospy.logwarn("Failed to read {}: {}".format(bag_path, e))

    if earliest_time is None:
        raise ValueError("No valid bag files found or all were unreadable.")

    # Convert to rospy.Time
    secs = int(earliest_time)
    nsecs = int((earliest_time - secs) * 1e9)
    return rospy.Time(secs, nsecs)


def get_first_timestamp(topic, gt_topic):


    gt_msg = rospy.wait_for_message(gt_topic, rospy.AnyMsg)
    msg = rospy.wait_for_message(topic, rospy.AnyMsg)


    # Try to deserialize the header if it exists
    msg_class, real_topic, _ = get_topic_class(topic)

    rospy.logwarn("Failed to read {}".format(topic))

    if not msg_class:
        raise ValueError("Cannot determine message type for topic: {}".format(topic))
    
    deserialized_msg = msg_class().deserialize(msg._buff)
    if not hasattr(deserialized_msg, 'header'):
        raise AttributeError("Message on topic {} has no header".format(topic))
    
    return deserialized_msg.header.stamp


class CutManager:
    def __init__(self):
        self.relay_processes = {}
        self.node_start_time = get_earliest_rosbag_timestamp()

        # Load parameters
        self.cut_config = rospy.get_param("cut_list")
        self.algo_topic = rospy.get_param("algo_topic")
        self.gt_topic = rospy.get_param("gt_topic")

        self.update_interval = rospy.Duration(0.1)  # Check every 0.1 second

        self.init_nodes() 

        self.node_start_time = get_first_timestamp(self.algo_topic, self.gt_topic)
        
        # Start periodic update
        self.timer = rospy.Timer(self.update_interval, self.update_nodes)
        rospy.loginfo("Cut manager started")

    def is_active(self, active_periods):
        """Check if current ROS time is within any active period"""
        current_time = (rospy.get_rostime() - self.node_start_time).to_sec()

        rospy.logwarn("Current timestamp {}".format(current_time))
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
            
        args = ["rosrun", "topic_tools", "relay", topic, "{}_cut".format(topic)]

        self.relay_processes[sensor] = subprocess.Popen(
            args,
            preexec_fn=os.setsid
        )

        rospy.loginfo("Started relay for {}".format(sensor))

    def stop_node(self, process_dict, sensor):
        """Stop a running node and its process group"""
        if sensor in process_dict:
            process = process_dict[sensor]
            try:
                os.killpg(os.getpgid(process.pid), signal.SIGTERM)
                process.wait()
                rospy.loginfo("Stopped {} node".format(sensor))
            except Exception as e:
                rospy.logwarn("Error killing {} process: {}".format(sensor, e))
            del process_dict[sensor]
            rospy.loginfo("Stopped {} node".format(sensor))

    def init_nodes(self):
        """Periodic update check"""
        for config in self.cut_config:
            sensor = config["sensor"]
            topic = config["topic"]
            
            self.start_relay_node(sensor, topic)

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