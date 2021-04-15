#!/usr/bin/env python3
import os
import sys
import unittest
import io
import threading
from importlib.util import spec_from_loader, module_from_spec
from importlib.machinery import SourceFileLoader 

spec = spec_from_loader("cycles", SourceFileLoader("cycles", "../cycles"))
cycles = module_from_spec(spec)
spec.loader.exec_module(cycles)

def feed_lines(grouper, lines):
    for line in lines:
        grouper.addLine(line)
    grouper.stop()

class LogCyclesTests(unittest.TestCase):
    def parse(self, lines, **kwargs):
        r, w = os.pipe()
        r, w = os.fdopen(r, 'r'), os.fdopen(w, 'w')
        g = cycles.Grouper(w, **kwargs)
        g.start()
        feeder = threading.Thread(target=feed_lines, args=(g, lines))
        feeder.start()
        result = r.read()
        r.close()
        feeder.join()
        self.assertEqual(g.status, 0)
        return result

    def test_basic(self):
        lines = ["a\n", "b\n", "c\n"]
        self.assertEqual(self.parse(lines), "".join(lines))
    def test_empty(self):
        lines = []
        self.assertEqual(self.parse(lines), "".join(lines))
    def test_singles(self):
        lines = (c + "\n" for c in "abbbc")
        result = (c + "\n" for c in "abc")
        self.assertEqual(self.parse(lines, strip=True), "".join(result))
    def test_singles2(self):
        lines = (c + "\n" for c in "abbbccccd")
        result = (c + "\n" for c in "abcd")
        self.assertEqual(self.parse(lines, strip=True), "".join(result))
    def test_nest(self):
        lines = (c + "\n" for c in "ababab")
        result = (c + "\n" for c in "ab")
        self.assertEqual(self.parse(lines, strip=True), "".join(result))
    def test_nest2(self):
        lines = (c + "\n" for c in "abababcabababc")
        result = (c + "\n" for c in "abc")
        self.assertEqual(self.parse(lines, strip=True), "".join(result))
    def test_nest3(self):
        lines = (c + "\n" for c in "abababcddabababcdd")
        result = (c + "\n" for c in "abcd")
        self.assertEqual(self.parse(lines, strip=True), "".join(result))
    def test_nest4(self):
        lines = (c + "\n" for c in "abababcddabababcddabababcddabababcddeabababcddabababcdd")
        result = (c + "\n" for c in "abcdeabcd")
        self.assertEqual(self.parse(lines, strip=True), "".join(result))

if __name__ == '__main__':
    unittest.main()

