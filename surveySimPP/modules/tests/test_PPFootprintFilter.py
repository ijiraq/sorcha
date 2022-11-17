import numpy as np
import pandas as pd 

from surveySimPP.modules.PPFootprintFilter import Detector, Footprint, radec2focalplane

from numpy.random import default_rng
rng = default_rng()

# detector tests
def dummy_detector():
    # detector covering the square [0, 1], [0, 1], in radians
    return Detector(np.array(((0, 1, 1, 0),(0, 0, 1, 1))))

def test_ison():
    detector = dummy_detector()
    points = rng.random((2, 10))
    points_ison = detector.ison(points)
    assert points_ison.shape == (10,)

# shelving these for the moment, need a better idea for what to test for
# main problem is that the area calculated is on a plane tangent to the unit sphere
# so area is not trivial for trivial corners
# maybe just assert that they give the same answer for a bunch of random points

def test_areas():
    detector = dummy_detector()
    pointsin = rng.random((2, 10))
    pointsout = rng.random((2, 10)) + 1
    for p in pointsin.T:
        pi = np.array(radec2focalplane(p[0], p[1], 0, 0))
        assert np.abs(detector.segmentedArea(pi) - detector.trueArea()) < 1e-12 # these are stricter tolerances than currently demanded in current use
    for p in pointsout.T:
        pi = np.array(radec2focalplane(p[0], p[1], 0, 0))
        assert np.abs(detector.segmentedArea(pi) - detector.trueArea()) > 1e-12


# def test_trueArea():
#     detector = dummy_detector()
#     assert detector.trueArea() == 1.0

# def test_segmentedArea():
#     detector = dummy_detector()
#     points = rng.random((2, 10))
#     for p in points.T:
#         assert detector.segmentedArea(p) == 1.0

# def test_sortCorners():
#     detector = dummy_detector()

# other functions

# assert that the shape of the output is fine
# more precise tests should be done manually 
# but checking for proper output will at least be
# a little useful in preventing people(me) from
# breaking things
def test_radec2focalplane():
    out = radec2focalplane(1., 1., 0., 0.)
    assert len(out)==2
