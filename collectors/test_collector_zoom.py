from zoom import Zoom
import unittest


class TestZoom(unittest.TestCase):
    def test_get_auth_key(self):
        try:
            zoomCollector = Zoom()
            zoomCollector.get_zoom_records(
                "https://zoom.us/rec/share/4CkpeBjt0nayyaCj2HSfwOMuSaC8i8XBCWY4RtigSbCL7s2LXsHC6W9r3vqqIG66.YZgT-lMNj1W1l4_C",
                "yeardream23!",
                False,
            )
        except Exception as e:
            self.fail(e)

    if __name__ == "__main__":
        unittest.main()
