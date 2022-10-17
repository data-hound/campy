"""
CamPy: Python-based multi-camera recording software.
Integrates machine vision camera APIs with ffmpeg real-time compression.
Outputs one MP4 video file and metadata files for each camera

"campy" is the main console. 
User inputs are loaded from config yaml file using a command line interface (CLI) 
configurator parses the config arguments (and default params) into "params" dictionary.
configurator assigns params to each camera stream in the "cam_params" dictionary.
	* Camera index is set by "cameraSelection".
	* If param is string, it is applied to all cameras.
	* If param is list of strings, it is assigned to each camera, ordered by camera index.
Camera streams are acquired and encoded in parallel using multiprocessing.

Usage: 
campy-acquire ./configs/campy_config.yaml
"""

import os, time, sys, logging, threading, queue
from collections import deque
import multiprocessing as mp
# from concurrent.futures import ProcessPoolExecutor as Puul
# from multiprocessing import freeze_support
from campy import writer, display, configurator
from campy.trigger import trigger
from campy.cameras import unicam
from campy.utils.utils import HandleKeyboardInterrupt
from arrayqueues.shared_arrays import ArrayQueue

def OpenSystems():
	# Configure parameters
	params = configurator.ConfigureParams()

	# Load Camera Systems and Devices
	systems = unicam.LoadSystems(params)
	systems = unicam.GetDeviceList(systems, params)

	# Start camera triggers if configured
	systems = trigger.StartTriggers(systems, params)

	return systems, params


def CloseSystems(systems, params):
	trigger.StopTriggers(systems, params)
	unicam.CloseSystems(systems, params)


def AcquireOneCamera(n_cam, systems, params):
    # Initialize param dictionary for this camera stream
    cam_params = configurator.ConfigureCamParams(systems, params, n_cam)
    print ("n_cam = ",n_cam)
    # buffer_in_mb = (cam_params["frameRate"] *cam_params["frameWidth"]*cam_params["frameHeight"]*cam_params["numCams"])/(1000.0*1000.0)
    # Initialize queues for display, video writer, and stop messages
    dispQueue = deque([], 2)
    writeQueue = deque()
    # writeQueue = ArrayQueue(int(buffer_in_mb))
    # writeQueue = mp.Queue()
    # stopReadQueue = mp.Queue(1)
    # stopWriteQueue = mp.Queue(1)
    stopReadQueue = deque([],1)
    stopWriteQueue = deque([],1)

    # Start image window display thread
    if cam_params["displayFrameRate"] > 0:
        threading.Thread(
            target = display.DisplayFrames,
            daemon = True,
            args = (cam_params, dispQueue,),
            ).start()

    # Start grabbing frames ("producer" thread)
    threading.Thread(
        target = unicam.GrabFrames,
        daemon = True,
        args = (cam_params, writeQueue, dispQueue, stopReadQueue, stopWriteQueue,),
        ).start()

    # Start multiple video file writer threads ("consumer" processes)
    # *********************** DOES NOT WORK  ************************
    # **** -> Getting
    # threading.Thread(
    # 	target = writer.AuxWriteFrames,
    # 	daemon = True,
    # 	args = (cam_params, writeQueue, stopReadQueue, stopWriteQueue,),
    # ).start()

    # threading.Thread(
    # 	target = writer.AuxWriteFrames,
    # 	daemon = True,
    # 	args = (cam_params, writeQueue, stopReadQueue, stopWriteQueue,),
    # ).start()
    
    # Main thread writer
    # writer.AuxWriteFramesMainThread(cam_params, writeQueue, stopReadQueue, stopWriteQueue)

    # Start video file writer (main "consumer" process)
    # if len(writeQueue) > 0:
    # 	writeQueue.popleft()
    
    writer.WriteFrames(cam_params, writeQueue, stopReadQueue, stopWriteQueue)
    # writer_process = mp.Process(group=None,
    #                             target=writer.WriteFrames,
    #                             name= 'writer_process',
    #                             args = (cam_params, writeQueue, stopReadQueue, stopWriteQueue),
    #                             #kwargs= {"gpuToUse": 0, "frameRate": 90},
    #                             daemon=True,
    #                             )
    # writer_process.start()
    # writer_process.join()

# Nuggets-1: https://stackoverflow.com/questions/49318306/python-threadpoolexecutor-not-executing-proper
#           Uses ThreadPoolExecutor - create a threadpool for each camera, and then spawn a sub-process
# Nuggets-2: https://stackoverflow.com/questions/6974695/python-process-pool-non-daemonic
#           Write custom Multiprocessing class to not have daemonic processes in the outer pool
def Main():
    # freeze_support()
    # mp.set_start_method('spawn')
    systems, params = OpenSystems()
    with HandleKeyboardInterrupt():
        # Acquire cameras in parallel with Windows- and Linux-compatible pool
        p = mp.get_context("spawn").Pool(params["numCams"])
        p.map_async(AcquireOneCamera,[range(params["numCams"]), systems, params]).get()
        # with Puul(params["numCams"]) as p:
        #     p.map(AcquireOneCamera,[(0,systems,params)])
        #     p.submit(AcquireOneCamera,[(0,systems,params)]).result()
        # #p = Puul(max_workers=1)#params["numCams"])
        
        #p.map(AcquireOneCamera,[int(1)])
        # AcquireOneCamera(0)

    # CloseSystems(systems, params)

# Open systems, creates global 'systems' and 'params' variables
# systems, params = OpenSystems()