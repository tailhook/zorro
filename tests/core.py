from .base import Test, passive

class Core(Test):
    test_timeout = 0.25

    @passive
    def test_future_cb_simple(self):
        f = self.z.Future(lambda:123)
        self.assertEquals(f.get(), 123)

    @passive
    def test_future_cb_to(self):
        f = self.z.Future(lambda:(self.z.sleep(0.1),234))
        self.assertEquals(f.get(), (None, 234))

    @passive
    def test_future_timeo_ok(self):
        f = self.z.Future(lambda:(self.z.sleep(0.1),234))
        self.assertEquals(f.get(timeout=0.2), (None, 234))

    @passive
    def test_future_timeo_raised(self):
        f = self.z.Future(lambda:(self.z.sleep(0.2),234))
        with self.assertRaises(self.z.TimeoutError):
            f.get(timeout=0.1)
        self.assertEquals(f.get(), (None, 234))

    @passive
    def test_future_user(self):
        f = self.z.Future()
        self.hub.do_spawn(lambda: f.set('hello'))
        self.assertEquals(f.get(), 'hello')

    @passive
    def test_future_user_to(self):
        f = self.z.Future()
        self.hub.do_spawn(lambda: (self.z.sleep(0.1), f.set('hello')))
        self.assertEquals(f.get(), 'hello')

    @passive
    def test_condition(self):
        cond = self.z.Condition()
        r = []
        self.hub.do_spawn(
            lambda: (r.append('hello'), cond.notify()))
        cond.wait()
        self.assertEquals(r, ['hello'])

    @passive
    def test_both(self):
        cond = self.z.Condition()
        f = self.z.Future()
        self.hub.do_spawn(
            lambda: (f.set('hello'), cond.notify()))
        self.assertEquals(f._value , self.z.core.FUTURE_PENDING)
        cond.wait()
        self.assertEquals(f._value, 'hello')

    @passive
    def test_condition_timeo(self):
        cond = self.z.Condition()
        r = []
        self.hub.do_spawn(lambda:
            (r.append('1'), self.z.sleep(0.2),
             r.append('3'), cond.notify()))
        cond.wait(timeout=0.1)
        r.append('2')
        cond.wait(timeout=0.2)
        self.assertEquals(r, ['1', '2', '3'])

    @passive
    def test_thrash(self):
        f = self.z.Future()
        self.hub.do_spawn(lambda: f.get())
        self.should_timeout = True # because nowhere to get future from

    @passive
    def test_leak(self):
        def leak():
            f = self.z.Future()
            self.hub.do_spawnhelper(lambda: f.get())
        self.hub.do_spawn(leak)

    @passive
    def test_lock(self):
        l = self.z.Lock()
        res = []
        def one():
            with l:
                res.append(1)
                self.z.sleep(0.1)
                res.append(2)
        def two():
            with l:
                res.append(3)
                self.z.sleep(0.05)
                res.append(4)
        self.hub.do_spawn(one)
        self.hub.do_spawn(two)
        self.z.sleep(0.2)
        self.assertEquals(res, [1,2,3,4])

if __name__ == '__main__':
    import unittest
    unittest.main()
