"""Tests for OpenMeteo Bounding Box Repository"""
import io
from decimal import Decimal
from pv_prospect.common.openmeteo_bounding_box_repo import (
    _parse_row,
    _parse_coordinate,
    _str_to_coordinate,
    build_openmeteo_bounding_box_repo,
    om_bounding_boxes_by_pv_site_id,
)
from pv_prospect.common.domain.location import Location
from pv_prospect.common.domain.bounding_box import BoundingBox


class TestStrToCoordinate:
    """Tests for the _str_to_coordinate helper function"""

    def test_positive_latitude_with_integer_part(self):
        """Test parsing positive latitude like 516004 -> 51.6004"""
        result = _str_to_coordinate('516004')
        assert result == Decimal('51.6004')

    def test_positive_longitude_with_integer_part(self):
        """Test parsing positive longitude like 07808 -> 0.7808"""
        result = _str_to_coordinate('07808')
        assert result == Decimal('0.7808')

    def test_negative_latitude_with_integer_part(self):
        """Test parsing negative latitude"""
        result = _str_to_coordinate('-516004')
        assert result == Decimal('-51.6004')

    def test_negative_longitude_with_integer_part(self):
        """Test parsing negative longitude like -41776 -> -4.1776"""
        result = _str_to_coordinate('-41776')
        assert result == Decimal('-4.1776')

    def test_coordinate_with_leading_zero(self):
        """Test coordinate with leading zero in decimal part"""
        result = _str_to_coordinate('515995')
        assert result == Decimal('51.5995')

    def test_negative_coordinate_with_small_integer_part(self):
        """Test negative coordinate like -41487 -> -4.1487"""
        result = _str_to_coordinate('-41487')
        assert result == Decimal('-4.1487')

    def test_coordinate_only_decimal_part(self):
        """Test coordinate with only decimal digits (less than 5 chars)"""
        result = _str_to_coordinate('1234')
        assert result == Decimal('0.1234')

    def test_negative_coordinate_only_decimal_part(self):
        """Test negative coordinate with only decimal digits"""
        result = _str_to_coordinate('-5678')
        assert result == Decimal('-0.5678')

    def test_very_small_coordinate(self):
        """Test very small coordinate like 123 -> 0.0123"""
        result = _str_to_coordinate('123')
        assert result == Decimal('0.0123')

    def test_large_latitude(self):
        """Test large latitude like 526404 -> 52.6404"""
        result = _str_to_coordinate('526404')
        assert result == Decimal('52.6404')


class TestParseCoordinate:
    """Tests for the _parse_coordinate function"""

    def test_parse_valid_coordinate_positive(self):
        """Test parsing coordinate string with positive values"""
        location = _parse_coordinate('516004_-41776')
        assert isinstance(location, Location)
        assert location.latitude == Decimal('51.6004')
        assert location.longitude == Decimal('-4.1776')

    def test_parse_valid_coordinate_mixed_sign(self):
        """Test parsing coordinate string with mixed signs"""
        location = _parse_coordinate('526404_07808')
        assert isinstance(location, Location)
        assert location.latitude == Decimal('52.6404')
        assert location.longitude == Decimal('0.7808')

    def test_parse_sw_corner_from_sample(self):
        """Test parsing SW corner from sample CSV"""
        location = _parse_coordinate('516004_-41776')
        assert location.latitude == Decimal('51.6004')
        assert location.longitude == Decimal('-4.1776')

    def test_parse_se_corner_from_sample(self):
        """Test parsing SE corner from sample CSV"""
        location = _parse_coordinate('515995_-41487')
        assert location.latitude == Decimal('51.5995')
        assert location.longitude == Decimal('-4.1487')

    def test_parse_nw_corner_from_sample(self):
        """Test parsing NW corner from sample CSV"""
        location = _parse_coordinate('516213_-42626')
        assert location.latitude == Decimal('51.6213')
        assert location.longitude == Decimal('-4.2626')

    def test_parse_ne_corner_from_sample(self):
        """Test parsing NE corner from sample CSV"""
        location = _parse_coordinate('516174_-41472')
        assert location.latitude == Decimal('51.6174')
        assert location.longitude == Decimal('-4.1472')


class TestParseRow:
    """Tests for the _parse_row function"""

    def test_parse_row_returns_tuple(self):
        """Test that _parse_row returns a tuple of (int, BoundingBox)"""
        row = {
            'pv_site_id': '42248',
            'sw': '516004_-41776',
            'se': '515995_-41487',
            'nw': '516213_-42626',
            'ne': '516174_-41472'
        }
        result = _parse_row(row)
        assert isinstance(result, tuple)
        assert len(result) == 2
        assert isinstance(result[0], int)
        assert isinstance(result[1], BoundingBox)

    def test_parse_row_correct_pv_site_id(self):
        """Test that pv_site_id is correctly parsed"""
        row = {
            'pv_site_id': '42248',
            'sw': '516004_-41776',
            'se': '515995_-41487',
            'nw': '516213_-42626',
            'ne': '516174_-41472'
        }
        pv_site_id, _ = _parse_row(row)
        assert pv_site_id == 42248

    def test_parse_row_correct_bounding_box_from_sample_1(self):
        """Test parsing first row from sample CSV"""
        row = {
            'pv_site_id': '42248',
            'sw': '516004_-41776',
            'se': '515995_-41487',
            'nw': '516213_-42626',
            'ne': '516174_-41472'
        }
        pv_site_id, bbox = _parse_row(row)

        assert pv_site_id == 42248
        assert bbox.sw.latitude == Decimal('51.6004')
        assert bbox.sw.longitude == Decimal('-4.1776')
        assert bbox.se.latitude == Decimal('51.5995')
        assert bbox.se.longitude == Decimal('-4.1487')
        assert bbox.nw.latitude == Decimal('51.6213')
        assert bbox.nw.longitude == Decimal('-4.2626')
        assert bbox.ne.latitude == Decimal('51.6174')
        assert bbox.ne.longitude == Decimal('-4.1472')

    def test_parse_row_correct_bounding_box_from_sample_2(self):
        """Test parsing second row from sample CSV"""
        row = {
            'pv_site_id': '89665',
            'sw': '526395_07603',
            'se': '526404_07808',
            'nw': '526600_07614',
            'ne': '526604_07799'
        }
        pv_site_id, bbox = _parse_row(row)

        assert pv_site_id == 89665
        assert bbox.sw.latitude == Decimal('52.6395')
        assert bbox.sw.longitude == Decimal('0.7603')
        assert bbox.se.latitude == Decimal('52.6404')
        assert bbox.se.longitude == Decimal('0.7808')
        assert bbox.nw.latitude == Decimal('52.6600')
        assert bbox.nw.longitude == Decimal('0.7614')
        assert bbox.ne.latitude == Decimal('52.6604')
        assert bbox.ne.longitude == Decimal('0.7799')


class TestBuildOpenMeteoBoundingBoxRepo:
    """Tests for the build_openmeteo_bounding_box_repo function"""

    def setup_method(self):
        """Clear the repository before each test"""
        om_bounding_boxes_by_pv_site_id.clear()

    def test_build_repo_from_sample_csv(self):
        """Test building repository from sample CSV data"""
        csv_content = """pv_site_id,sw,se,nw,ne
42248,516004_-41776,515995_-41487,516213_-42626,516174_-41472
89665,526395_07603,526404_07808,526600_07614,526604_07799
"""
        csv_stream = io.StringIO(csv_content)

        build_openmeteo_bounding_box_repo(csv_stream)

        assert len(om_bounding_boxes_by_pv_site_id) == 2
        assert 42248 in om_bounding_boxes_by_pv_site_id
        assert 89665 in om_bounding_boxes_by_pv_site_id

    def test_build_repo_correct_bounding_boxes(self):
        """Test that bounding boxes are correctly stored in repository"""
        csv_content = """pv_site_id,sw,se,nw,ne
42248,516004_-41776,515995_-41487,516213_-42626,516174_-41472
89665,526395_07603,526404_07808,526600_07614,526604_07799
"""
        csv_stream = io.StringIO(csv_content)

        build_openmeteo_bounding_box_repo(csv_stream)

        bbox1 = om_bounding_boxes_by_pv_site_id[42248]
        assert bbox1.sw.latitude == Decimal('51.6004')
        assert bbox1.sw.longitude == Decimal('-4.1776')

        bbox2 = om_bounding_boxes_by_pv_site_id[89665]
        assert bbox2.sw.latitude == Decimal('52.6395')
        assert bbox2.sw.longitude == Decimal('0.7603')

    def test_build_repo_only_once(self):
        """Test that repository is only built once (idempotent)"""
        csv_content = """pv_site_id,sw,se,nw,ne
42248,516004_-41776,515995_-41487,516213_-42626,516174_-41472
"""
        csv_stream1 = io.StringIO(csv_content)

        build_openmeteo_bounding_box_repo(csv_stream1)
        assert len(om_bounding_boxes_by_pv_site_id) == 1

        # Try to build again with different data
        csv_content2 = """pv_site_id,sw,se,nw,ne
99999,526395_07603,526404_07808,526600_07614,526604_07799
"""
        csv_stream2 = io.StringIO(csv_content2)

        build_openmeteo_bounding_box_repo(csv_stream2)

        # Should still have only the first entry
        assert len(om_bounding_boxes_by_pv_site_id) == 1
        assert 42248 in om_bounding_boxes_by_pv_site_id
        assert 99999 not in om_bounding_boxes_by_pv_site_id

    def test_build_repo_from_empty_csv(self):
        """Test building repository from CSV with only headers"""
        csv_content = """pv_site_id,sw,se,nw,ne
"""
        csv_stream = io.StringIO(csv_content)

        build_openmeteo_bounding_box_repo(csv_stream)

        assert len(om_bounding_boxes_by_pv_site_id) == 0

    def test_build_repo_with_single_entry(self):
        """Test building repository with a single entry"""
        csv_content = """pv_site_id,sw,se,nw,ne
12345,516004_-41776,515995_-41487,516213_-42626,516174_-41472
"""
        csv_stream = io.StringIO(csv_content)

        build_openmeteo_bounding_box_repo(csv_stream)

        assert len(om_bounding_boxes_by_pv_site_id) == 1
        assert 12345 in om_bounding_boxes_by_pv_site_id

        bbox = om_bounding_boxes_by_pv_site_id[12345]
        assert isinstance(bbox, BoundingBox)
        assert len(bbox.get_vertices()) == 4

