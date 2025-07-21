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


def get_first_timestamp(topic):

    for _ in range(0,2):
        msg = rospy.wait_for_message(topic, rospy.AnyMsg)
        
    # Try to deserialize the header if it exists
    msg_class, real_topic, _ = get_topic_class(topic)

    rospy.logwarn("Failed to read {topic}".format(topic))

    if not msg_class:
        raise ValueError("Cannot determine message type for topic: {}".format(topic))
    
    deserialized_msg = msg_class().deserialize(msg._buff)
    if not hasattr(deserialized_msg, 'header'):
        raise AttributeError("Message on topic {} has no header".format(topic))
    
    return deserialized_msg.header.stamp

class DropManager:
    def __init__(self):
        self.drop_processes = {}
        self.relay_processes = {}
        #self.node_start_time = rospy.get_rostime()  # ROS time at node startup
        self.node_start_time = get_earliest_rosbag_timestamp()
        
        # Load parameters
        self.drop_config = rospy.get_param("drop_list")
        self.algo_topic = rospy.get_param("algo_topic")
        self.update_interval = rospy.Duration(0.1)  # Check every 0.1 second

        self.init_nodes() 

        self.node_start_time = get_first_timestamp(self.algo_topic)
        
        # Start periodic update
        self.timer = rospy.Timer(self.update_interval, self.update_nodes)
        rospy.loginfo("Drop manager started")

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

    def start_drop_node(self, sensor, topic, drop_rate):
        """Start a topic_tools/drop node"""
        if sensor in self.drop_processes:
            return
            
        args = [
            "rosrun", "topic_tools", "drop",
            topic, str(drop_rate[0]), str(drop_rate[1]), "{}_drop".format(topic)
        ]

        self.drop_processes[sensor] = subprocess.Popen(
            args,
            preexec_fn=os.setsid
        )

        rospy.loginfo("Started drop node for {}".format(sensor))

    def start_relay_node(self, sensor, topic):
        """Start a relay node when drops are inactive"""
        if sensor in self.relay_processes:
            return
            
        args = ["rosrun", "topic_tools", "relay", topic, "{}_drop".format(topic)]

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
                rospy.logwarn("Error killing {} process: {}".format(sensor,e))
            del process_dict[sensor]
            rospy.loginfo("Stopped {} node".format(sensor))

    def init_nodes(self):
        """Initial update check"""
        for config in self.drop_config:
            sensor = config["sensor"]
            topic = config["topic"]

            self.stop_node(self.drop_processes, sensor)
            self.start_relay_node(sensor, topic)

    def update_nodes(self):
        """Periodic update check"""
        for config in self.drop_config:
            sensor = config["sensor"]
            topic = config["topic"]
            drop_rate = config["drop_rate"]
            active_periods = config["active_periods"]

            if self.is_active(active_periods):
                self.stop_node(self.relay_processes, sensor)
                self.start_drop_node(sensor, topic, drop_rate)
            else:
                self.stop_node(self.drop_processes, sensor)
                self.start_relay_node(sensor, topic)

    def shutdown(self):
        """Cleanup on shutdown"""
        for sensor in list(self.drop_processes.keys()):
            self.stop_node(self.drop_processes, sensor)
        for sensor in list(self.relay_processes.keys()):
            self.stop_node(self.relay_processes, sensor)

if __name__ == "__main__":
    rospy.init_node("dynamic_drop_manager")
    manager = DropManager()
    rospy.on_shutdown(manager.shutdown)
    rospy.spin()