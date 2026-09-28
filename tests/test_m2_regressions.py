import json, socket, selectors, threading, time, unittest
from unittest.mock import patch
from distributed_system.server.replica_conns import ReplicaConnections
from distributed_system.common import BufferedJsonConnection

class Checks(unittest.TestCase):
 def setup_client(self):
  c=ReplicaConnections('C1', {}); socks=c.alive; peers={}
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
  started=time.monotonic(); self.assertTrue(c.send_and_collect(c._last_completed + 1, 'Hello')); self.assertLess(time.monotonic()-started,.5)
 def test_late_duplicate_then_current_on_same_socket(self):
  c,s,p=self.setup_client(); c._last_completed=1
  self.reply(p['S1'],'S1',1)
  def later():
   time.sleep(.05); self.reply(p['S1'],'S1',2)
  t=threading.Thread(target=later); t.start()
  with patch('distributed_system.server.replica_conns.log') as log:
   self.assertTrue(c.send_and_collect(c._last_completed + 1, 'Hello'))
   log.assert_any_call('request_num 1: Discarded duplicate reply from S1',kind='info')
  t.join(); self.assertEqual(c._last_completed,2)
 def test_eof_removed_healthy_continues(self):
  c,s,p=self.setup_client(); p['S1'].close(); self.reply(p['S2'],'S2',1)
  self.assertTrue(c.send_and_collect(c._last_completed + 1, 'Hello')); self.assertNotIn('S1',s)
 def test_deadline_bounds_partial(self):
  c,s,p=self.setup_client(); p['S1'].sendall(b'{'); now=time.monotonic()
  self.assertFalse(c._receive_until(now+.1,1)); self.assertLess(time.monotonic()-now,.4)
 def test_partial_duplicate_survives_request_boundary(self):
  c,s,p=self.setup_client()
  payload=(json.dumps(dict(type='reply',client_id='C1',replica_id='S1',request_num=1))+'\n').encode()
  p['S1'].sendall(payload[:10]); self.reply(p['S2'],'S2',1)
  self.assertIsNotNone(c.send_and_collect(1,'Hello'))
  p['S1'].sendall(payload[10:]); self.reply(p['S1'],'S1',2)
  with patch('distributed_system.server.replica_conns.log') as log:
   self.assertEqual(c.send_and_collect(2,'Hello')['request_num'],2)
   log.assert_any_call('request_num 1: Discarded duplicate reply from S1',kind='info')
 def test_wrong_identity_is_not_delivered(self):
  c,s,p=self.setup_client(); self.reply(p['S1'],'S2',1)
  self.assertIsNone(c._receive_until(time.monotonic()+.03,1))
  self.assertEqual(c._last_completed,0)
if __name__ == '__main__':
 unittest.main()
