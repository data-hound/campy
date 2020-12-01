"""

"""
import sys
import time
import logging
import numpy as np

import matplotlib as mpl
mpl.use('Qt5Agg') # disregard qtapp warning...
import matplotlib.pyplot as plt

def draw_figure(num):
	mpl.rcParams['toolbar'] = 'None' 

	figure = plt.figure(num)
	ax = plt.axes([0,0,1,1], frameon=False)

	plt.axis('off')
	plt.autoscale(tight=True)
	plt.ion()

	imageWindow = ax.imshow(np.zeros((1,1,3), dtype='uint8'), 
		interpolation='none')

	figure.canvas.draw()
	plt.show(block=False)

	return figure, imageWindow

def DisplayFrames(cam_params, dispQueue):
	n_cam = cam_params['n_cam']
	
	if sys.platform == "win32" and cam_params['cameraMake'] == 'basler':
		# Pylon image window is handled by cameras.basler.cam
		pass
	else:
		figure, imageWindow = draw_figure(n_cam+1)
		while(True):
			try:
				if dispQueue:
					img = dispQueue.popleft()
					try:
						imageWindow.set_data(img)
						figure.canvas.draw()
						figure.canvas.flush_events()
					except Exception as e:
						logging.error('Caught exception: {}'.format(e))
				else:
					time.sleep(0.01)
			except KeyboardInterrupt:
				break
		plt.close(figure)