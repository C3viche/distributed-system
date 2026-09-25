import json, socket, selectors, threading, time, unittest
from unittest.mock import patch
from distributed_system.client.client import Client
from distributed_system.common import BufferedJsonConnection

class Checks(unittest.TestCase):
 def setup_client(self):
  c=Client('C1',2); c._selector=selectors.DefaultSelector(); c._connections={}; socks={}; peers={}
  for name in ('S1','S2'):
   a,b=socket.socketpair(); socks[name]=a; peers[name]=b
   c._connections[a]=BufferedJsonConnection(a); c._selector.register(a,selectors.EVENT_READ,name)
  self.addCleanup(c._selector.close)
  for s in [*socks.values(),*peers.values()]: self.addCleanup(s.close)
  return c,socks,peers
 def reply(self,p,name,n):
  p.sendall((json.dumps(dict(type='reply',client_id='C1',replica_id=name,request_num=n,state=n))+'\n').encode())
 def test_partial_peer_does_not_block_healthy(self):
  c,s,p=self.setup_client(); p['S1'].sendall(b'{"type":'); self.reply(p['S2'],'S2',1)
  started=time.monotonic(); self.assertTrue(c._send_and_await_reply(s)); self.assertLess(time.monotonic()-started,.5)
 def test_late_duplicate_then_current_on_same_socket(self):
  c,s,p=self.setup_client(); c.request_num=2
  self.reply(p['S1'],'S1',1)
  def later():
   time.sleep(.05); self.reply(p['S1'],'S1',2)
  t=threading.Thread(target=later); t.start()
  with patch('distributed_system.client.client.log') as log:
   self.assertTrue(c._send_and_await_reply(s))
   log.assert_any_call('request_num 1: Discarded duplicate reply from S1',kind='info')
  t.join(); self.assertEqual(c.request_num,3)
 def test_eof_removed_healthy_continues(self):
  c,s,p=self.setup_client(); p['S1'].close(); self.reply(p['S2'],'S2',1)
  self.assertTrue(c._send_and_await_reply(s)); self.assertNotIn('S1',s)
 def test_deadline_bounds_partial(self):
  c,s,p=self.setup_client(); p['S1'].sendall(b'{'); now=time.monotonic()
  self.assertFalse(c._receive_until(s,now+.1,1)); self.assertLess(time.monotonic()-now,.4)
if __name__ == '__main__':
 unittest.main()
