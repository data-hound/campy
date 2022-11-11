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
Camera streams are acquired and encoded in parallel using multiprocess.

Usage: 
campy-acquire ./configs/campy_config.yaml
"""

from multiprocess.dummy import Manager
import os, time, sys, logging, threading, queue
from collections import deque
import multiprocess as mp
from concurrent.futures import ProcessPoolExecutor as Puul
from multiprocess import freeze_support
from campy import writer, display, configurator
from campy.trigger import trigger
from campy.cameras import unicam
from campy.utils.utils import HandleKeyboardInterrupt
from arrayqueues.shared_arrays import ArrayQueue
import time
import copy
from functools import partial
import dill

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


def AcquireOneCamera(args): #n_cam, params): #n_cam): #, systems, params):
    # Initialize param dictionary for this camera stream
    n_cam = args[0]
    params = args[1]
    cam_params = configurator.ConfigureCamParams(systems, params, n_cam)
    # global cam_params_list
    print("Initializing for acquisition")
    # print(cam_params_list)
    # cam_params = cam_params_list[n_cam]
    # print("Trying to copy....")
    # deep_params = copy.deepcopy(cam_params)
    # print (not deep_params)
    print(cam_params)
    buffer_in_mb = (cam_params["frameRate"] *cam_params["frameWidth"]*cam_params["frameHeight"]*cam_params["numCams"]*3)/(1000.0*1000.0)
    # Initialize queues for display, video writer, and stop messages
    dispQueue = deque([], 2)
    # writeQueue = deque()
    # writeQueue = ArrayQueue(int(buffer_in_mb))
    writeQueue = mp.Queue()
    stopReadQueue = mp.Queue(1)
    stopWriteQueue = mp.Queue(1)
    # stopReadQueue = deque([],1)
    # stopWriteQueue = deque([],1)
    
    drop_keys = ["device", "camera"]
    dropped_params = {k:v for k,v in cam_params.items() if k not in drop_keys}
    deep_params = copy.deepcopy(dropped_params)
    print("Location of cam_params = ", id(cam_params))
    print("Location of dropped_params = ", id(dropped_params))
    print("Location of deep_params = ", id(deep_params))
    print(deep_params)
    
    print("Check StopQue: ", stopReadQueue.empty())

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
    
    # print("Trying to copy....")
    # deep_params = copy.deepcopy(cam_params)
    # print (not deep_params)
    
    # time.sleep(20)
    time.sleep(1)
    # print (copy.deepcopy(cam_params))
    # writer.WriteFrames(cam_params, writeQueue, stopReadQueue, stopWriteQueue)
    writer.WriteFrames(deep_params, writeQueue, stopReadQueue, stopWriteQueue)
    print("Check whether dill pickles: writer.MultiProcessedWriteFrames ",dill.pickles(writer.MultiProcessedWriteFrames, safe = True))
    # print("Check whether dill pickles: writeQueue ",dill.pickles(writeQueue, safe = True))
    # print("Check whether dill pickles: stopReadQueue ",dill.pickles(stopReadQueue, safe = True))
    # print("Check whether dill pickles: stopWriteQueue ",dill.pickles(stopWriteQueue, safe = True))
    # print("Check whether dill pickles: writer_params ",dill.pickles(deep_params, safe = True))
    # with mp.Manager() as deputy:
    #     writer_params = deputy.dict()
    #     writer_params = copy.deepcopy(deep_params)
    #     writer_process = mp.Process(group=None,
    #                                 target=writer.MultiProcessedWriteFrames, #writer.WriteFrames,
    #                                 name= 'writer_process',
    #                                 args = ((writer_params, writeQueue, stopReadQueue, stopWriteQueue)),
    #                                 #kwargs= {"gpuToUse": 0, "frameRate": 90},
    #                                 daemon=True,
    #                                 )
    #     writer_process.start()
    #     writeQueue.get()
        # with Puul(1) as writer_process:
        #     writer_process.map(writer.MultiProcessedWriteFrames,(writer_params, writeQueue, stopReadQueue, stopWriteQueue))
    # writer_process.join()
    # with Puul(1) as writer_process:
    #     writer_process.map(writer.MultiProcessedWriteFrames,(cam_params, writeQueue, stopReadQueue, stopWriteQueue))

def get_cam_params_list(systems, params, cp_list):
    # cam_params_list = []
    print(params["numCams"])
    for i in range(params["numCams"]):
        print("Getting deep copied cam_params")
        # cam_params_list.append(configurator.ConfigureCamParams(systems, None, i))
        cp_list[i] = (configurator.ConfigureCamParams(systems, None, i))
        print(i)
    return cp_list #cam_params_list

# Nuggets-1: https://stackoverflow.com/questions/49318306/python-threadpoolexecutor-not-executing-proper
#           Uses ThreadPoolExecutor - create a threadpool for each camera, and then spawn a sub-process
# Nuggets-2: https://stackoverflow.com/questions/6974695/python-process-pool-non-daemonic
#           Write custom Multiprocessing class to not have daemonic processes in the outer pool
def Main():
    freeze_support()
    # mp.set_start_method('spawn')
    # systems, params = OpenSystems()
    # global cam_params_list
    with mp.Manager() as manager:
        my_params = manager.dict()
        my_params = copy.deepcopy(params)
        # cam_params_list = list(range(params["numCams"]))
        # cam_params_list = get_cam_params_list(systems, params, cam_params_list)
        
        args = [(i, copy.deepcopy(my_params)) for i in range(params["numCams"])]
        with HandleKeyboardInterrupt():
            # Acquire cameras in parallel with Windows- and Linux-compatible pool
            print("Starting...")
            # print("using cam_params: ", cam_params_list)
            print("Using args = ", args)
            # p = mp.get_context("spawn").Pool(params["numCams"])
            # p.map_async(AcquireOneCamera,range(params["numCams"])).get()
            with Puul(params["numCams"]) as p:
                print("Starting.....")
                acquire_nth_cam = partial(AcquireOneCamera, params= my_params)
            # with mp.Pool(params["numCams"]) as p:
                # p.map(AcquireOneCamera,range(params["numCams"]))
                # p.map(acquire_nth_cam,range(params["numCams"]))
                p.map(AcquireOneCamera,args)
                # p.map(AcquireOneCamera,cam_params_list)
            #     p.map(AcquireOneCamera,[(0,systems,params)])
            #     p.submit(AcquireOneCamera,[(0,systems,params)]).result()
            # #p = Puul(max_workers=1)#params["numCams"])
            
            #p.map(AcquireOneCamera,[int(1)])
            # AcquireOneCamera(0)

    # CloseSystems(systems, params)

# Open systems, creates global 'systems' and 'params' variables
systems, params = OpenSystems()
# cam_params_list = []