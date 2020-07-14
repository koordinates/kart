import itertools
import math
from . import vector_tile_pb2

# Constants

## Complex Value Type
CV_TYPE_STRING = 0
CV_TYPE_FLOAT = 1
CV_TYPE_DOUBLE = 2
CV_TYPE_UINT = 3
CV_TYPE_SINT = 4
CV_TYPE_INLINE_UINT = 5
CV_TYPE_INLINE_SINT = 6
CV_TYPE_BOOL_NULL = 7
CV_TYPE_LIST = 8
CV_TYPE_MAP = 9
CV_TYPE_LIST_DOUBLE = 10

## Complex Value Bool/Null Meaning
CV_NULL = 0
CV_BOOL_FALSE = 1
CV_BOOL_TRUE = 2

DEFAULT_SPLINE_DEGREE = 2

# Python3 Compatability
try:
    unicode
    other_str = unicode
except NameError:
    other_str = bytes
    long = int

def zig_zag_encode(val):
    return (int(val) << 1) ^ (int(val) >> 31)

def zig_zag_encode_64(val):
    return (int(val) << 1) ^ (int(val) >> 63)

def zig_zag_decode(val):
    return ((val >> 1) ^ (-(val & 1)))

def command_integer(cmd_id, count):
    return (cmd_id & 0x7) | (count << 3);

def command_move_to(count):
    return command_integer(1, count)

def command_line_to(count):
    return command_integer(2, count)

def command_close_path():
    return command_integer(7,1)

def get_command_id(command_integer):
    return command_integer & 0x7;

def get_command_count(command_integer):
    return command_integer >> 3

def next_command_move_to(command_integer):
    return get_command_id(command_integer) == 1

def next_command_line_to(command_integer):
    return get_command_id(command_integer) == 2

def next_command_close_path(command_integer):
    return get_command_id(command_integer) == 7

def get_inline_value_id(complex_value):
    return complex_value & 0x0F;

def get_inline_value_parameter(complex_value):
    return complex_value >> 4;

def complex_value_integer(cmd_id, param):
    return (cmd_id & 0x0F) | (param << 4);

class Float(float):

    def __new__(self, *args, **kwargs):
        x = float(*args, **kwargs)
        vm = vector_tile_pb2.Tile.Value()
        vm.float_value = x
        return float.__new__(self, vm.float_value)
    def __init__(self, *args, **kwargs):
        float.__init__(*args, **kwargs)

class UInt(long):

    def __new__(self, *args, **kwargs):
        return long.__new__(self, *args, **kwargs)
    def __init__(self, *args, **kwargs):
        long.__init__(*args, **kwargs)

def scaling_calculation(precision, min_float, max_float):
    if min_float >= max_float:
        raise Exception("Invalid Float Range")
    if precision > (max_float - min_float):
        raise Exception("Precision value too large for range")
    if precision < 0:
        raise Exception("Precision can not be a negative value")
    lbits = math.ceil(math.log((max_float - min_float) / precision, 2) + 1.0)
    #lbytes = int(math.ceil(lbits / 8.0))
    bPow = int(math.ceil(math.log(max_float - min_float, 2)))
    #dPow = 8*lbytes - 1
    dPow = lbits - 1
    sF = pow(2.0, (dPow - bPow))
    sR = pow(2.0, (bPow - dPow))
    return {'sF': sF, 'sR': sR, 'base': min_float }

class FloatList(list):

    def __init__(self, *args, **kwargs):
        if len(args) < 0:
            raise Exception("FloatList initialization requires first argument to be Scaling object")
        if isinstance(args[0], FloatList):
            self._scaling = args[0]._scaling
        elif isinstance(args[0], Scaling):
            self._scaling = args[0]
            args = tuple(args[1:])
        else:
            raise Exception("Unknown object passed to FloatList, first argument must be a Scaling object")
        if isinstance(args[0], list):
            new_list = []
            for v in args[0]:
                if v is None:
                    new_list.append(v)
                elif isinstance(v, float):
                    new_list.append(self._scaling.encode_value(v))
                elif isinstance(v, int) or isinstance(v, long):
                    new_list.append(self._scaling.encode_value(float(v)))
            new_args = [new_list]
            new_args.extend(args[1:])
            args = tuple(new_args)
        super(FloatList, self).__init__(*args, **kwargs)

    def append_value(self, value):
        if value is None:
            self.append(None)
        else:
            self.append(self._scaling.encode_value(value))

    def get_value_at(self, index):
        if self[index] is None:
            return self[index]
        return self._scaling.decode_value(self[index])

    def set_value_at(self, index, value):
        if value is None:
            self[index] = None
        else:
            self[index] = self._scaling.encode_value(value)

    def get_all_values(self):
        vals = []
        for v in self:
            if v is None:
                vals.append(None)
            else:
                vals.append(self._scaling.decode_value(v))
        return vals

    @property
    def index(self):
        return self._scaling.index

class FeatureAttributes(object):

    def __init__(self, feature, layer, is_geometric=False):
        self._feature = feature
        self._layer = layer
        self._attr = {}
        self._attr_current = False
        self._is_geometric = is_geometric

    def _encode_attr(self):
        if self._layer._inline_attributes:
            if self._is_geometric:
                self._feature.geometric_attributes[:] = self._layer.add_attributes(self._attr, True)
            else:
                self._feature.attributes[:] = self._layer.add_attributes(self._attr, False)
        else:
            self._feature.tags[:] = self._layer.add_attributes(self._attr)
        self._attr_current = True

    def _decode_attr(self):
        if not self._attr_current:
            if self._layer._inline_attributes:
                if self._is_geometric:
                    if len(self._feature.geometric_attributes) == 0:
                        self._attr = {}
                    else:
                        self._attr = self._layer.get_attributes(self._feature.geometric_attributes, True)
                else:
                    if len(self._feature.attributes) == 0:
                        self._attr = {}
                    else:
                        self._attr = self._layer.get_attributes(self._feature.attributes)
            else:
                if len(self._feature.tags) == 0:
                    self._attr = {}
                else:
                    self._attr = self._layer.get_attributes(self._feature.tags)
            self._attr_current = True

    def __len__(self):
        self._decode_attr()
        return len(self._attr)

    def __getitem__(self, key):
        self._decode_attr()
        if not isinstance(key, str) and not isinstance(key, other_str):
            raise TypeError("Keys must be of type str")
        return self._attr[key]

    def __delitem__(self, key):
        self._decode_attr()
        del self._attr[key]
        self._encode_attr()

    def __setitem__(self, key, value):
        if not isinstance(key, str) and not isinstance(key, other_str):
            raise TypeError("Keys must be of type str or other_str")
        self._decode_attr()
        self._attr[key] = value
        self._encode_attr()

    def __iter__(self):
        self._decode_attr()
        return self._attr.__iter__()

    def __eq__(self, other):
        self._decode_attr()
        if isinstance(other, dict):
            return self._attr == other
        elif isinstance(other, FeatureAttributes):
            other._decode_attr()
            return self._attr == other._attr
        return False

    def __str__(self):
        self._decode_attr()
        return self._attr.__str__()

    def __contains__(self, key):
        self._decode_attr()
        return self._attr.__contains__(key)

    def set(self, attr):
        self._attr = dict(attr)
        self._encode_attr()

class Feature(object):

    def __init__(self, feature, layer, has_elevation=None):
        self._feature = feature
        self._layer = layer
        if has_elevation is None:
            if len(self._feature.elevation) != 0:
                self._has_elevation = True
            else:
                self._has_elevation = False
        else:
            if has_elevation and self._layer.version < 3:
                raise Exception("Layers of version 1 or 2 can not have elevation data in features")
            self._has_elevation = has_elevation

        self._reset_cursor()
        self._attributes = FeatureAttributes(feature, layer, is_geometric=False)
        if self._layer._inline_attributes:
            self._geometric_attributes = FeatureAttributes(feature, layer, is_geometric=True)
        else:
            self._geometric_attributes = {}

    def _reset_cursor(self):
        self.cursor = []
        if self._has_elevation:
            self.cursor[:3] = itertools.repeat(0, 3)
        else:
            self.cursor[:2] = itertools.repeat(0, 2)
        self._cursor_at_end = False

    def _encode_point(self, pt, cmd_list, elevation_list):
        cmd_list.append(zig_zag_encode(int(pt[0]) - self.cursor[0]))
        cmd_list.append(zig_zag_encode(int(pt[1]) - self.cursor[1]))
        self.cursor[0] = int(pt[0])
        self.cursor[1] = int(pt[1])
        if self._has_elevation:
            if self._layer._elevation_scaling is None:
                elevation_list.append(int(pt[2]) - self.cursor[2])
                self.cursor[2] = int(pt[2])
            else:
                new_pt = self._layer._elevation_scaling.encode_value(pt[2])
                elevation_list.append(new_pt - self.cursor[2])
                self.cursor[2] = new_pt


    def _decode_point(self, integers):
        self.cursor[0] = self.cursor[0] + zig_zag_decode(integers[0])
        self.cursor[1] = self.cursor[1] + zig_zag_decode(integers[1])
        out = [self.cursor[0], self.cursor[1]]
        if len(integers) > 2:
            self.cursor[2] = self.cursor[2] + integers[2]
            if self._layer._elevation_scaling is None:
                out.append(self.cursor[2])
            else:
                out.append(self._layer._elevation_scaling.decode_value(self.cursor[2]))
        return out

    def _points_equal(self, pt1, pt2):
        if pt1[0] is not pt2[0] or pt1[1] is not pt2[1] or (self._has_elevation and pt1[2] is not pt2[2]):
            return False
        return True

    @property
    def has_elevation(self):
        return self._has_elevation

    @property
    def attributes(self):
        return self._attributes

    @attributes.setter
    def attributes(self, attrs):
        self._attributes.set(attrs)

    @property
    def geometric_attributes(self):
        return self._geometric_attributes

    @geometric_attributes.setter
    def geometric_attributes(self, attrs):
        if not self._layer._inline_attributes:
            raise Exception("Can not set geometric attributes for none inline attributes configured layer.")
        self._geometric_attributes.set(attrs)

    @property
    def id(self):
        if self._feature.HasField('id'):
            return self._feature.id;
        elif self._feature.HasField('string_id'):
            return self._feature.string_id;
        return None

    @id.setter
    def id(self, id_val):
        if isinstance(id_val, int):
            self._feature.id = id_val
            if self._feature.HasField('string_id'):
                self._feature.ClearField('string_id')
        elif self._layer.version >= 3:
            self._feature.string_id = id_val
            if self._feature.HasField('id'):
                self._feature.ClearField('id')
        else:
            raise Exception("Can not set string id for features using version 2 or below of the VT specification")

    def clear_geometry(self):
        self.has_geometry = False
        self._reset_cursor()
        self._feature.ClearField('geometry')
        self._feature.ClearField('elevation')

class PointFeature(Feature):

    def __init__(self, feature, layer, has_elevation=None):
        super(PointFeature, self).__init__(feature, layer, has_elevation)
        if feature.type is not vector_tile_pb2.Tile.POINT:
            feature.type = vector_tile_pb2.Tile.POINT
        self.type = 'point'
        self._num_points = 0

    def add_points(self, points):
        if not isinstance(points, list):
            raise Exception("Invalid point geometry")
        if not self._cursor_at_end:
            # Use geometry retrieval process to move cursor to proper position
            pts = self.get_points()
            self._num_points = len(pts)
        if len(points) < 1:
            return
        multi_point = isinstance(points[0], list)
        if multi_point:
            num_commands = len(points)
        else:
            num_commands = 1

        cmd_list = []
        if self._has_elevation:
            elevation_list = []
        else:
            elevation_list = None
        if self._num_points == 0:
            cmd_list.append(command_move_to(num_commands))
        try:
            if multi_point:
                for i in range(num_commands):
                    self._encode_point(points[i], cmd_list, elevation_list)
            else:
                self._encode_point(points, cmd_list, elevation_list)
        except Exception as e:
            self._reset_cursor()
            raise e
        if self._num_points != 0:
            self._num_points = self._num_points + num_commands
            self._feature.geometry[0] = command_move_to(self._num_points)
        self._feature.geometry.extend(cmd_list)
        if elevation_list:
            try:
                self._feature.elevation.extend(elevation_list)
            except ValueError:
                raise Exception("Elevation scaling results in value outside of value range of sint32, reduce elevation scaling precision.")

    def get_points(self, no_elevation=False):
        points = []
        self._reset_cursor()
        geom = iter(self._feature.geometry)
        if self.has_elevation and not no_elevation:
            elevation = iter(self._feature.elevation)
        try:
            current_command = next(geom)
            while next_command_move_to(current_command):
                for i in range(get_command_count(current_command)):
                    if self.has_elevation and not no_elevation:
                        points.append(self._decode_point([next(geom), next(geom), next(elevation)]))
                    else:
                        points.append(self._decode_point([next(geom), next(geom)]))
                current_command = next(geom)
        except StopIteration:
            pass
        self._cursor_at_end = True
        return points

    def get_geometry(self, no_elevation = False):
        return self.get_points(no_elevation)

class LineStringFeature(Feature):

    def __init__(self, feature, layer, has_elevation=None):
        super(LineStringFeature, self).__init__(feature, layer, has_elevation)
        if feature.type is not vector_tile_pb2.Tile.LINESTRING:
            feature.type = vector_tile_pb2.Tile.LINESTRING
        self.type = 'line_string'

    def add_line_string(self, linestring):
        num_commands = len(linestring)
        if num_commands < 2:
            raise Exception("Error adding linestring, less then 2 points provided")
        if not self._cursor_at_end:
            # Use geometry retrieval process to move cursor to proper position
            self.get_line_strings()
        if self._has_elevation:
            elevation_list = []
        else:
            elevation_list = None
        try:
            cmd_list = []
            cmd_list.append(command_move_to(1))
            self._encode_point(linestring[0], cmd_list, elevation_list)
            cmd_list.append(command_line_to(num_commands - 1))
            for i in range(1, num_commands):
                self._encode_point(linestring[i], cmd_list, elevation_list)
        except Exception as e:
            self._reset_cursor()
            raise e
        self._feature.geometry.extend(cmd_list)
        if elevation_list:
            try:
                self._feature.elevation.extend(elevation_list)
            except ValueError:
                raise Exception("Elevation scaling results in value outside of value range of sint32, reduce elevation scaling precision.")

    def get_line_strings(self, no_elevation=False):
        line_strings = []
        line_string = []
        self._reset_cursor()
        geom = iter(self._feature.geometry)
        if self.has_elevation and not no_elevation:
            elevation = iter(self._feature.elevation)
        try:
            current_command = next(geom)
            while next_command_move_to(current_command):
                line_string = []
                if get_command_count(current_command) != 1:
                    raise Exception("Command move_to has command count not equal to 1 in a line string")
                if self.has_elevation and not no_elevation:
                    line_string.append(self._decode_point([next(geom), next(geom), next(elevation)]))
                else:
                    line_string.append(self._decode_point([next(geom), next(geom)]))
                current_command = next(geom)
                if not next_command_line_to(current_command):
                    raise Exception("Command move_to not followed by a line_to command in a line string")
                while next_command_line_to(current_command):
                    for i in range(get_command_count(current_command)):
                        if self.has_elevation and not no_elevation:
                            line_string.append(self._decode_point([next(geom), next(geom), next(elevation)]))
                        else:
                            line_string.append(self._decode_point([next(geom), next(geom)]))
                    current_command = next(geom)
                if len(line_string) > 1:
                    line_strings.append(line_string)
        except StopIteration:
            if len(line_string) > 1:
                line_strings.append(line_string)
            pass
        self._cursor_at_end = True
        return line_strings

    def get_geometry(self, no_elevation=False):
        return self.get_line_strings(no_elevation)

class PolygonFeature(Feature):

    def __init__(self, feature, layer, has_elevation=None):
        super(PolygonFeature, self).__init__(feature, layer, has_elevation)
        if feature.type is not vector_tile_pb2.Tile.POLYGON:
            feature.type = vector_tile_pb2.Tile.POLYGON
        self.type = 'polygon'

    def add_ring(self, ring):
        if not self._cursor_at_end:
            # Use geometry retrieval process to move cursor to proper position
            self.get_rings()
        num_commands = len(ring)
        if num_commands < 3:
            raise Exception("Error adding ring to polygon, too few points")
        if self._points_equal(ring[0], ring[-1]):
            num_commands = num_commands - 1
        if num_commands < 3:
            raise Exception("Error adding ring to polygon, too few points with last point closing")
        cmd_list = []
        if self._has_elevation:
            elevation_list = []
        else:
            elevation_list = None
        try:
            cmd_list.append(command_move_to(1))
            self._encode_point(ring[0], cmd_list, elevation_list)
            cmd_list.append(command_line_to(num_commands - 1))
            for i in range(1, num_commands):
                self._encode_point(ring[i], cmd_list, elevation_list)
            cmd_list.append(command_close_path())
        except Exception as e:
            self._reset_cursor()
            raise e
        self._feature.geometry.extend(cmd_list)
        if elevation_list:
            try:
                self._feature.elevation.extend(elevation_list)
            except ValueError:
                raise Exception("Elevation scaling results in value outside of value range of sint32, reduce elevation scaling precision.")

    def get_rings(self, no_elevation=False):
        rings = []
        ring = []
        self._reset_cursor()
        geom = iter(self._feature.geometry)
        if self.has_elevation and not no_elevation:
            elevation = iter(self._feature.elevation)
        try:
            current_command = next(geom)
            while next_command_move_to(current_command):
                ring = []
                if get_command_count(current_command) != 1:
                    raise Exception("Command move_to has command count not equal to 1 in a line string")
                if self.has_elevation and not no_elevation:
                    ring.append(self._decode_point([next(geom), next(geom), next(elevation)]))
                else:
                    ring.append(self._decode_point([next(geom), next(geom)]))
                current_command = next(geom)
                while next_command_line_to(current_command):
                    for i in range(get_command_count(current_command)):
                        if self.has_elevation and not no_elevation:
                            ring.append(self._decode_point([next(geom), next(geom), next(elevation)]))
                        else:
                            ring.append(self._decode_point([next(geom), next(geom)]))
                    current_command = next(geom)
                if not next_command_close_path(current_command):
                    raise Exception("Polygon not closed with close_path command")
                ring.append(ring[0])
                if len(ring) > 3:
                    rings.append(ring)
                current_command = next(geom)
        except StopIteration:
            pass
        self._cursor_at_end = True
        return rings

    def _is_ring_clockwise(self, ring):
        area = 0.0
        for i in range(len(ring) - 1):
            area += (float(ring[i][0]) * float(ring[i+1][1])) - (float(ring[i][1]) * float(ring[i+1][0]))
        return area < 0.0

    def get_polygons(self, no_elevation=False):
        rings = self.get_rings(no_elevation)
        polygons = []
        polygon = []
        for ring in rings:
            if not self._is_ring_clockwise(ring):
                if len(polygon) != 0:
                    polygons.append(polygon)
                polygon = []
                polygon.append(ring)
            elif len(polygon) != 0:
                polygon.append(ring)
        if len(polygon) != 0:
            polygons.append(polygon)
        return polygons

    def get_geometry(self, no_elevation=False):
        return self.get_polygons(no_elevation)

class SplineFeature(Feature):

    def __init__(self, feature, layer, has_elevation=None, degree=None):
        super(SplineFeature, self).__init__(feature, layer, has_elevation)
        if feature.type is not vector_tile_pb2.Tile.SPLINE:
            feature.type = vector_tile_pb2.Tile.SPLINE
        self.type = 'spline'
        if self._feature.HasField('spline_degree'):
            self._degree = self._feature.spline_degree
        elif degree is None or degree == DEFAULT_SPLINE_DEGREE:
            self._degree = DEFAULT_SPLINE_DEGREE
        else:
            self._degree = degree
            self._feature.spline_degree = degree

    def add_spline(self, control_points, knots):
        num_commands = len(control_points)
        if num_commands < 2:
            raise Exception("Error adding control points, less then 2 points provided")
        if not isinstance(knots, FloatList):
            raise Exception("Knot values must be provided in the form of a FloatList")
        num_knots = len(knots)
        if num_knots != (num_commands + self._degree + 1):
            raise Exception("The length of knots must be equal to the length of control points + degree + 1")
        cmd_list = []
        if self._has_elevation:
            elevation_list = []
        else:
            elevation_list = None
        try:
            cmd_list.append(command_move_to(1))
            self._encode_point(control_points[0], cmd_list, elevation_list)
            cmd_list.append(command_line_to(num_commands - 1))
            for i in range(1, num_commands):
                self._encode_point(control_points[i], cmd_list, elevation_list)
        except Exception as e:
            self._reset_cursor()
            raise e
        self._feature.geometry.extend(cmd_list)
        if elevation_list:
            try:
                self._feature.elevation.extend(elevation_list)
            except ValueError:
                raise Exception("Elevation scaling results in value outside of value range of sint32, reduce elevation scaling precision.")
        values, length = self._layer._add_inline_float_list(knots)
        values.insert(0, complex_value_integer(CV_TYPE_LIST_DOUBLE, length))
        self._feature.spline_knots.extend(values)

    @property
    def degree(self):
        return self._degree

    def get_splines(self, no_elevation=False):
        splines = []
        self._reset_cursor()
        geom = iter(self._feature.geometry)
        knots_itr = iter(self._feature.spline_knots)
        if self._has_elevation and not no_elevation:
            elevation = iter(self._feature.elevation)
        try:
            current_command = next(geom)
            while next_command_move_to(current_command):
                control_points = []
                if get_command_count(current_command) != 1:
                    raise Exception("Command move_to has command count not equal to 1 in a line string")
                if self._has_elevation and not no_elevation:
                    control_points.append(self._decode_point([next(geom), next(geom), next(elevation)]))
                else:
                    control_points.append(self._decode_point([next(geom), next(geom)]))
                current_command = next(geom)
                while next_command_line_to(current_command):
                    for i in range(get_command_count(current_command)):
                        if self._has_elevation and not no_elevation:
                            control_points.append(self._decode_point([next(geom), next(geom), next(elevation)]))
                        else:
                            control_points.append(self._decode_point([next(geom), next(geom)]))
                    current_command = next(geom)
                if len(control_points) > 1:
                    splines.append([control_points])
        except StopIteration:
            if len(control_points) > 1:
                splines.append([control_points])
            pass

        try:
            for i in range(len(splines)):
                complex_value = next(knots_itr)
                val_id = get_inline_value_id(complex_value)
                param = get_inline_value_parameter(complex_value)
                if val_id == CV_TYPE_LIST_DOUBLE:
                    knots = self._layer._get_inline_float_list(knots_itr, param)
                num_cp = len(splines[i][0])
                num_knots = len(knots)
                if num_knots == (num_cp + self._degree + 1):
                    splines[i].append(knots)
        except StopIteration:
            pass
        self._cursor_at_end = True
        return splines

    def get_geometry(self, no_elevation=False):
        return self.get_splines(no_elevation)

class Scaling(object):

    def __init__(self, scaling_object, index = None, offset = None, multiplier = None, base = None):
        self._scaling_object = scaling_object
        self._index = index
        if offset is not None or multiplier is not None or base is not None:
            self._init_from_values(offset, multiplier, base)
        else:
            self._init_from_object()

    def _init_from_object(self):
        if self._scaling_object.HasField('offset'):
            self._offset = self._scaling_object.offset
        else:
            self._offset = 0
        if self._scaling_object.HasField('multiplier'):
            self._multiplier = self._scaling_object.multiplier
        else:
            self._multiplier = 1.0
        if self._scaling_object.HasField('base'):
            self._base = self._scaling_object.base
        else:
            self._base = 0.0

    def _init_from_values(self, offset, multiplier, base):
        if offset is not None and offset != 0:
            self._scaling_object.offset = int(offset)
            self._offset = int(offset)
        else:
            self._offset = 0
        if multiplier is not None and multiplier != 1.0:
            self._scaling_object.multiplier = float(multiplier)
            self._multiplier = float(multiplier)
        else:
            self._multiplier = 1.0
        if base is not None and base != 0.0:
            self._scaling_object.base = float(base)
            self._base = float(base)
        else:
            self._base = 0.0

    @property
    def type(self):
        return self._type

    @property
    def offset(self):
        return self._offset

    @property
    def multiplier(self):
        return self._multiplier

    @property
    def base(self):
        return self._base

    @property
    def index(self):
        return self._index

    def encode_value(self, value):
        return int(round((value - self._base) / self._multiplier)) - self._offset

    def decode_value(self, value):
        return self._multiplier * (value + self._offset) + self._base

class Layer(object):

    def __init__(self, layer, name = None, version = None, x = None, y = None, zoom = None, legacy_attributes=False):
        self._layer = layer
        self._features = []
        if name:
            self._layer.name = name
        if version:
            self._layer.version = version
        elif not self._layer.HasField('version'):
            self._layer.version = 2

        self._keys = []
        self._decode_keys()
        if self.version > 2 and len(self._layer.values) == 0 and not legacy_attributes:
            self._inline_attributes = True
            self._string_values = []
            self._float_values = []
            self._double_values = []
            self._int_values = []
            self._decode_inline_values()
        else:
            self._inline_attributes = False
            self._values = []
            self._decode_values()

        self._decode_attribute_scalings()

        if x is not None and y is not None and zoom is not None:
            self.set_tile_location(zoom, x, y)

        if self._layer.HasField('elevation_scaling'):
            self._elevation_scaling = Scaling(self._layer.elevation_scaling)
        else:
            self._elevation_scaling = None

        self._build_features()

    def _decode_attribute_scalings(self):
        self._attribute_scalings = []
        for i in range(len(self._layer.attribute_scalings)):
            self._attribute_scalings.append(Scaling(self._layer.attribute_scalings[i], index=i))

    def _decode_values(self):
        for val in self._layer.values:
            if val.HasField('bool_value'):
                self._values.append(val.bool_value)
            elif val.HasField('string_value'):
                self._values.append(val.string_value)
            elif val.HasField('float_value'):
                self._values.append(val.float_value)
            elif val.HasField('double_value'):
                self._values.append(val.double_value)
            elif val.HasField('int_value'):
                self._values.append(val.int_value)
            elif val.HasField('uint_value'):
                self._values.append(val.uint_value)
            elif val.HasField('sint_value'):
                self._values.append(val.sint_value)

    def _decode_inline_values(self):
        for val in self._layer.string_values:
            self._string_values.append(val)
        for val in self._layer.float_values:
            self._float_values.append(Float(val))
        for val in self._layer.double_values:
            self._double_values.append(val)
        for val in self._layer.int_values:
            self._int_values.append(val)

    def _decode_keys(self):
        for key in self._layer.keys:
            self._keys.append(key)

    def _build_features(self):
        for feature in self._layer.features:
            if feature.type == vector_tile_pb2.Tile.POINT:
                self._features.append(PointFeature(feature, self))
            elif feature.type == vector_tile_pb2.Tile.LINESTRING:
                self._features.append(LineStringFeature(feature, self))
            elif feature.type == vector_tile_pb2.Tile.POLYGON:
                self._features.append(PolygonFeature(feature, self))
            elif feature.type == vector_tile_pb2.Tile.SPLINE:
                self._features.append(SplineFeature(feature, self))

    def add_elevation_scaling(self, offset=0, multiplier=1.0, base=0.0, min_value=None, max_value=None, precision=None):
        if self.version < 3:
            raise Exception("Can not add elevation scaling to Version 2 or below Vector Tiles.")
        if min_value is not None and max_value is not None and precision is not None:
            out = scaling_calculation(precision, float(min_value), float(max_value))
            offset = 0
            base = out['base']
            multiplier = out['sR']
        self._elevation_scaling = Scaling(self._layer.elevation_scaling, offset=offset, multiplier=multiplier, base=base)
        return self._elevation_scaling

    def add_attribute_scaling(self, offset=0, multiplier=1.0, base=0.0, min_value=None, max_value=None, precision=None):
        if self.version < 3:
            raise Exception("Can not add attribute scaling to Version 2 or below Vector Tiles.")
        if not self._inline_attributes:
            raise Exception("Can not add attribute scaling to Version 3 or greater layers that do not support inline attributes")
        if min_value is not None and max_value is not None and precision is not None:
            out = scaling_calculation(precision, float(min_value), float(max_value))
            offset = 0
            base = out['base']
            multiplier = out['sR']
        index = len(self._attribute_scalings)
        self._attribute_scalings.append(Scaling(self._layer.attribute_scalings.add(), index=index, offset=offset, multiplier=multiplier, base=base))
        return self._attribute_scalings[index]

    def add_point_feature(self, has_elevation=False):
        self._features.append(PointFeature(self._layer.features.add(), self, has_elevation=has_elevation))
        return self._features[-1]

    def add_line_string_feature(self, has_elevation=False):
        self._features.append(LineStringFeature(self._layer.features.add(), self, has_elevation=has_elevation))
        return self._features[-1]

    def add_polygon_feature(self, has_elevation=False):
        self._features.append(PolygonFeature(self._layer.features.add(), self, has_elevation=has_elevation))
        return self._features[-1]

    def add_spline_feature(self, has_elevation=False, degree=None):
        if self.version < 3:
            raise Exception("Can not add splines to Version 2 or below Vector Tiles.")
        self._features.append(SplineFeature(self._layer.features.add(), self, has_elevation=has_elevation, degree=degree))
        return self._features[-1]

    @property
    def features(self):
        return self._features

    @property
    def name(self):
        return self._layer.name

    @name.setter
    def name(self, name):
        self._layer.name = name

    @property
    def extent(self):
        if self._layer.HasField('extent'):
            return self._layer.extent
        return 4096

    @extent.setter
    def extent(self, extent):
        self._layer.extent = extent

    @property
    def version(self):
        if self._layer.HasField('version'):
            return self._layer.version
        return 2

    @property
    def elevation_scaling(self):
        return self._elevation_scaling

    @property
    def attribute_scalings(self):
        return self._attribute_scalings

    @property
    def x(self):
        if self._layer.HasField('tile_x'):
            return self._layer.tile_x
        else:
            return None

    @property
    def y(self):
        if self._layer.HasField('tile_y'):
            return self._layer.tile_y
        else:
            return None

    @property
    def zoom(self):
        if self._layer.HasField('tile_zoom'):
            return self._layer.tile_zoom
        else:
            return None

    def set_tile_location(self, zoom, x, y):
        if self.version < 3:
            raise Exception("Can not add tile location to Version 2 or below Vector Tiles.")
        if zoom < 0 or zoom > 50:
            raise Exception("Please use a zoom level between 0 and 50")
        if x < 0 or x > (2**zoom - 1):
            raise Exception("Tile x value outside of possible values given zoom level")
        if y < 0 or y > (2**zoom - 1):
            raise Exception("Tile y value outside of possible values given zoom level")
        self._layer.tile_x = x
        self._layer.tile_y = y
        self._layer.tile_zoom = zoom

    def get_attributes(self, int_list, list_only=False):
        if not self._inline_attributes:
            attributes = {}
            for i in range(0,len(int_list),2):
                attributes[self._keys[int_list[i]]] = self._values[int_list[i+1]]
            return attributes
        else:
            return self._get_inline_map_attributes(iter(int_list), limit=None, list_only=list_only)

    def _get_inline_value(self, complex_value, value_itr):
        val_id = get_inline_value_id(complex_value)
        param = get_inline_value_parameter(complex_value)
        if val_id == CV_TYPE_STRING:
            return self._string_values[param]
        elif val_id == CV_TYPE_FLOAT:
            return self._float_values[param]
        elif val_id == CV_TYPE_DOUBLE:
            return self._double_values[param]
        elif val_id == CV_TYPE_SINT:
            return zig_zag_decode(self._int_values[param])
        elif val_id == CV_TYPE_UINT:
            return self._int_values[param]
        elif val_id == CV_TYPE_INLINE_UINT:
            return param
        elif val_id == CV_TYPE_INLINE_SINT:
            return zig_zag_decode(param)
        elif val_id == CV_TYPE_BOOL_NULL:
            if param == CV_BOOL_FALSE:
                return False
            elif param == CV_BOOL_TRUE:
                return True
            else:
                return None
        elif val_id == CV_TYPE_LIST:
            return self._get_inline_list_attributes(value_itr, param)
        elif val_id == CV_TYPE_MAP:
            return self._get_inline_map_attributes(value_itr, param)
        elif val_id == CV_TYPE_LIST_DOUBLE:
            return self._get_inline_float_list(value_itr, param)
        else:
            raise Exception("Unknown value type in inline value")

    def _get_inline_map_attributes(self, value_itr, limit = None, list_only=False):
        attr_map = {}
        if limit == 0:
            return attr_map
        count = 0
        for key in value_itr:
            try:
                val = next(value_itr)
            except StopIteration:
                break
            if list_only:
                val_id = get_inline_value_id(val)
                if val_id != CV_TYPE_LIST and val_id != CV_TYPE_LIST_DOUBLE:
                    raise Exception("Invalid value type top level in geometric_attributes of feature, must be a list type")
            attr_map[self._keys[key]] = self._get_inline_value(val, value_itr)
            count = count + 1
            if limit is not None and count >= limit:
                break
        return attr_map

    def _get_inline_list_attributes(self, value_itr, limit = None):
        attr_list = []
        if limit == 0:
            return attr_list
        count = 0
        for val in value_itr:
            attr_list.append(self._get_inline_value(val, value_itr))
            count = count + 1
            if limit is not None and count >= limit:
                break
        return attr_list

    def _get_inline_float_list(self, value_itr, limit = None):
        index = next(value_itr)
        if index < 0 and index >= len(self._attribute_scalings):
            raise Exception("Invalid attribute scaling index")
        scaling = self._attribute_scalings[index]
        attr_list = []
        if limit == 0:
            return attr_list
        count = 0
        cursor = 0
        for val in value_itr:
            if val == 0:
                attr_list.append(None)
            else:
                cursor = cursor + zig_zag_decode(val - 1)
                attr_list.append(scaling.decode_value(cursor))
            count = count + 1
            if limit is not None and count >= limit:
                break
        return attr_list

    
    def _is_in_values(self, value):
        for e_v in self._values:
            if type(value) == type(e_v) and value == e_v:
                return True
        return False


    def _add_legacy_attributes(self, attrs):
        tags = []
        remove = []
        for k,v in attrs.items():
            if not isinstance(k, str) and not isinstance(k, other_str):
                remove.append(k)
                continue
            if not self._is_in_values(v):
                if isinstance(v,bool):
                    val = self._layer.values.add()
                    val.bool_value = v
                elif (isinstance(v,str)) or (isinstance(v,other_str)):
                    val = self._layer.values.add()
                    val.string_value = v
                elif isinstance(v,UInt) and v >= 0:
                    val = self._layer.values.add()
                    val.uint_value = v
                elif isinstance(v,int) or isinstance(v,long):
                    val = self._layer.values.add()
                    if v >= 0:
                        val.int_value = v
                    else:
                        val.sint_value = v
                elif isinstance(v,Float):
                    val = self._layer.values.add()
                    val.float_value = v
                elif isinstance(v,float):
                    val = self._layer.values.add()
                    val.double_value = v
                else:
                    remove.append(k)
                    continue
                self._values.append(v)
                value_index = len(self._values) - 1
            else:
                value_index = self._values.index(v)

            if k not in self._keys:
                self._layer.keys.append(k)
                self._keys.append(k)
            tags.append(self._keys.index(k))
            tags.append(value_index)
        for k in remove:
            del attrs[k]
        return tags

    def _add_inline_value(self, v):
        if v is None:
            return complex_value_integer(CV_TYPE_BOOL_NULL, CV_NULL)
        elif isinstance(v, bool):
            if v == True:
                return complex_value_integer(CV_TYPE_BOOL_NULL, CV_BOOL_TRUE)
            else:
                return complex_value_integer(CV_TYPE_BOOL_NULL, CV_BOOL_FALSE)
        elif isinstance(v,str) or isinstance(v,other_str):
            try:
                index = self._string_values.index(v)
                return complex_value_integer(CV_TYPE_STRING, index)
            except ValueError:
                self._string_values.append(v)
                self._layer.string_values.append(v)
                return complex_value_integer(CV_TYPE_STRING, len(self._string_values) - 1)
        elif isinstance(v,UInt) and v >= 0:
            if v >= 2**56:
                try:
                    index = self._int_values.index(v)
                    return complex_value_integer(CV_TYPE_UINT, index)
                except ValueError:
                    self._int_values.append(v)
                    self._layer.int_values.append(v)
                    return complex_value_integer(CV_TYPE_UINT, len(self._int_values) - 1)
            else:
                return complex_value_integer(CV_TYPE_INLINE_UINT, v)
        elif isinstance(v,int) or isinstance(v, long):
            if v >= 2**55 or v <= -2**55:
                zz_v = zig_zag_encode_64(v)
                try:
                    index = self._int_values.index(zz_v)
                    return complex_value_integer(CV_TYPE_SINT, index)
                except ValueError:
                    self._int_values.append(zz_v)
                    self._layer.int_values.append(zz_v)
                    return complex_value_integer(CV_TYPE_SINT, len(self._int_values) - 1)
            else:
                return complex_value_integer(CV_TYPE_INLINE_SINT, zig_zag_encode_64(v))
        elif isinstance(v, Float):
            try:
                index = self._float_values.index(v)
                return complex_value_integer(CV_TYPE_FLOAT, index)
            except ValueError:
                self._float_values.append(v)
                self._layer.float_values.append(v)
                return complex_value_integer(CV_TYPE_FLOAT, len(self._float_values) - 1)
        elif isinstance(v, float):
            try:
                index = self._double_values.index(v)
                return complex_value_integer(CV_TYPE_DOUBLE, index)
            except ValueError:
                self._double_values.append(v)
                self._layer.double_values.append(v)
                return complex_value_integer(CV_TYPE_DOUBLE, len(self._double_values) - 1)
        elif isinstance(v,FloatList):
            values, length = self._add_inline_float_list(v)
            values.insert(0, complex_value_integer(CV_TYPE_LIST_DOUBLE, length))
            return values
        elif isinstance(v,list):
            values, length = self._add_inline_list_attributes(v)
            if not values:
                return None
            values.insert(0, complex_value_integer(CV_TYPE_LIST, length))
            return values
        elif isinstance(v, dict):
            values, length = self._add_inline_map_attributes(v)
            if not values:
                return None
            values.insert(0, complex_value_integer(CV_TYPE_MAP, length))
            return values
        return None

    def _add_inline_float_list(self, attrs):
        delta_values = [attrs.index]
        length = len(attrs)
        cursor = 0
        for v in attrs:
            if v is None:
                delta_values.append(0)
            else:
                delta_values.append(zig_zag_encode_64(v - cursor) + 1)
                cursor = v
        return delta_values, length

    def _add_inline_list_attributes(self, attrs):
        complex_values = []
        length = len(attrs)
        remove = []
        for v in attrs:
            val = self._add_inline_value(v)
            if val is None:
                remove.append(v)
                continue
            if isinstance(val, list):
                complex_values.extend(val)
            else:
                complex_values.append(val)
        if remove:
            length = length - len(remove)
            data[:] = [x for x in data if x not in remove]
        return complex_values, length

    def _add_inline_map_attributes(self, attrs, list_only=False):
        complex_values = []
        length = len(attrs)
        remove = []
        for k,v in attrs.items():
            if not isinstance(k, str) and not isinstance(k, other_str):
                remove.append(k)
                continue
            if list_only and not isinstance(v, list) and not isinstance(v, FloatList):
                remove.append(k)
                continue
            val = self._add_inline_value(v)
            if val is None:
                remove.append(k)
                continue
            try:
                key_val = self._keys.index(k)
            except ValueError:
                self._layer.keys.append(k)
                self._keys.append(k)
                key_val = len(self._keys) - 1
            complex_values.append(key_val)
            if isinstance(val, list):
                complex_values.extend(val)
            else:
                complex_values.append(val)
        length = length - len(remove)
        for k in remove:
            del attrs[k]
        return complex_values, length

    def add_attributes(self, attrs, list_only=False):
        if self._inline_attributes:
            values, length = self._add_inline_map_attributes(attrs, list_only)
            return values
        elif list_only:
            return []
        else:
            return self._add_legacy_attributes(attrs)

class VectorTile(object):

    def __init__(self, tile = None):
        self._layers = []
        if tile:
            if (isinstance(tile,str)) or (isinstance(tile,other_str)):
                self._tile = vector_tile_pb2.Tile()
                self._tile.ParseFromString(tile)
            else:
                self._tile = tile
            self._build_layers()
        else:
            self._tile = vector_tile_pb2.Tile()

    def __str__(self):
        return self._tile.__str__()

    def _build_layers(self):
        for layer in self._tile.layers:
            self._layers.append(Layer(layer))

    def serialize(self):
        return self._tile.SerializeToString()

    def add_layer(self, name, version = None, x = None, y = None, zoom = None, legacy_attributes=False):
        self._layers.append(Layer(self._tile.layers.add(), name, version=version, x=x, y=y, zoom=zoom, legacy_attributes=legacy_attributes))
        return self._layers[-1]

    @property
    def layers(self):
        return self._layers
