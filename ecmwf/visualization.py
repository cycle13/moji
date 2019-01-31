#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
author:     yetao.lu
date:       2018/12/14
description:EC数据的可视化，气温，降水
"""
import pygrib,os,numpy
from osgeo import gdal,gdalnumeric, ogr
from PIL import Image, ImageDraw
import operator
def visualize(ecfile,shapefile,outpath):
    grbs=pygrib.open(ecfile)
    # for grb in grbs:
    #     print grb
    grb_2t = grbs.select(name='2 metre temperature')
    tempArray = grb_2t[0].values
    print tempArray,tempArray.shape
    pic=arrayTotif(tempArray,outpath)
    print pic
    main(shapefile,pic,outpath)

#
#  EDIT: this is basically an overloaded
#  version of the gdal_array.OpenArray passing in xoff, yoff explicitly
#  so we can pass these params off to CopyDatasetInfo
#
def OpenArray(array, prototype_ds=None, xoff=0, yoff=0):
    ds = gdal.Open(gdalnumeric.GetArrayFilename(array))
    
    if ds is not None and prototype_ds is not None:
        if type(prototype_ds).__name__ == 'str':
            prototype_ds = gdal.Open(prototype_ds)
        if prototype_ds is not None:
            gdalnumeric.CopyDatasetInfo(prototype_ds, ds, xoff=xoff, yoff=yoff)
    return ds


def histogram(a, bins=range(0, 256)):
    """
    Histogram function for multi-dimensional array.
    a = array
    bins = range of numbers to match
    """
    fa = a.flat
    n = gdalnumeric.searchsorted(gdalnumeric.sort(fa), bins)
    n = gdalnumeric.concatenate([n, [len(fa)]])
    hist = n[1:] - n[:-1]
    return hist

def stretch(a):
    """
    Performs a histogram stretch on a gdalnumeric array image.
    """
    hist = histogram(a)
    im = arrayToImage(a)
    lut = []
    for b in range(0, len(hist), 256):
        # step size
        step = reduce(operator.add, hist[b:b+256]) / 255
        # create equalization lookup table
        n = 0
        for i in range(256):
            lut.append(n / step)
            n = n + hist[i+b]
        im = im.point(lut)
    return imageToArray(im)

def imageToArray(i):
    """
    Converts a Python Imaging Library array to a
    gdalnumeric image.
    """
    a = gdalnumeric.fromstring(i.tobytes(), 'b')
    a.shape = i.im.size[1], i.im.size[0]
    return a

def arrayTotif(a,outpath):
    tiffile=os.path.join(outpath,'temp.tif')
    im=Image.fromarray(a.astype(numpy.float32))
    im.save(tiffile)
    return tiffile
def arrayToImage(a):
    """
    Converts a gdalnumeric array to a
    Python Imaging Library Image.
    """
    i = Image.frombytes('L', (a.shape[1], a.shape[0]),
                        (a.astype('b')).tobytes())
    return i

def world2Pixel(geoMatrix, x, y):
    """
    Uses a gdal geomatrix (gdal.GetGeoTransform()) to calculate
    the pixel location of a geospatial coordinate
    """
    ulX = geoMatrix[0]
    ulY = geoMatrix[3]
    xDist = geoMatrix[1]
    pixel = int((x - ulX) / xDist)
    line = int((ulY - y) / xDist)
    return (pixel, line)
def main(shapefile_path, raster_path,outpath):
    # Load the source data as a gdalnumeric array
    srcArray = gdalnumeric.LoadFile(raster_path)
    
    # Also load as a gdal image to get geotransform
    # (world file) info
    srcImage = gdal.Open(raster_path)
    geoTrans = srcImage.GetGeoTransform()
    
    # Create an OGR layer from a boundary shapefile
    shapef = ogr.Open(shapefile_path)
    lyr = shapef.GetLayer(
        os.path.split(os.path.splitext(shapefile_path)[0])[1])
    poly = lyr.GetNextFeature()
    
    # Convert the layer extent to image pixel coordinates
    minX, maxX, minY, maxY = lyr.GetExtent()
    ulX, ulY = world2Pixel(geoTrans, minX, maxY)
    lrX, lrY = world2Pixel(geoTrans, maxX, minY)
    
    # Calculate the pixel size of the new image
    pxWidth = 3600
    pxHeight = 1801
    print srcArray,srcArray.shape,ulY,lrY,ulX,lrX
    clip = srcArray[:, ulY:lrY, ulX:lrX]
    
    #
    # EDIT: create pixel offset to pass to new image Projection info
    #
    xoffset = ulX
    yoffset = ulY
    print "Xoffset, Yoffset = ( %f, %f )" % (xoffset, yoffset)
    
    # Create a new geomatrix for the image
    geoTrans = list(geoTrans)
    geoTrans[0] = minX
    geoTrans[3] = maxY
    
    # Map points to pixels for drawing the
    # boundary on a blank 8-bit,
    # black and white, mask image.
    points = []
    pixels = []
    geom = poly.GetGeometryRef()
    pts = geom.GetGeometryRef(0)
    for p in range(pts.GetPointCount()):
        points.append((pts.GetX(p), pts.GetY(p)))
    for p in points:
        pixels.append(world2Pixel(geoTrans, p[0], p[1]))
    rasterPoly = Image.new("L", (pxWidth, pxHeight), 1)
    rasterize = ImageDraw.Draw(rasterPoly)
    rasterize.polygon(pixels, 0)
    mask = imageToArray(rasterPoly)
    
    # Clip the image using the mask
    clip = gdalnumeric.choose(mask,
                              (clip, 0)).astype(gdalnumeric.uint8)
    
    # This image has 3 bands so we stretch each one to make them
    # visually brighter
    for i in range(3):
        clip[i, :, :] = stretch(clip[i, :, :])
    
    # Save new tiff
    #
    #  EDIT: instead of SaveArray, let's break all the
    #  SaveArray steps out more explicity so
    #  we can overwrite the offset of the destination
    #  raster
    #
    ### the old way using SaveArray
    #
    # gdalnumeric.SaveArray(clip, "OUTPUT.tif", format="GTiff", prototype=raster_path)
    #
    ###
    #
    gtiffDriver = gdal.GetDriverByName('GTiff')
    if gtiffDriver is None:
        raise ValueError("Can't find GeoTiff Driver")
    tiffile=os.path.join(outpath,"beijing.tif")
    gtiffDriver.CreateCopy(tiffile,
                           OpenArray(clip, prototype_ds=raster_path,
                                     xoff=xoffset, yoff=yoffset)
                           )
    
    # Save as an 8-bit jpeg for an easy, quick preview
    clip = clip.astype(gdalnumeric.uint8)
    jpgfile=os.path.join(outpath,"beijing.jpg")
    gdalnumeric.SaveArray(clip, jpgfile, format="JPEG")
    
    gdal.ErrorReset()

if __name__ == "__main__":
    ecfile='/Users/yetao.lu/Desktop/mos/new/D1D05210000052112001'
    shpfile='/Users/yetao.lu/chinashp/省界_region.shp'
    outpath='/Users/yetao.lu/2017'
    visualize(ecfile,shpfile,outpath)