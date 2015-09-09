# encoding=utf-8
__author__ = 'mk'

'''
本 app 简单实现 STP。步骤为：
1. 获取 switches 和 links 的 Topology.
2. 使用 Kruskal 构造 STP。
3. 对于不在 STP 中的 link 的 port, 安装 OF flow 进行 Drop.
4. 由于有 learning switch, 无需安装额外的 flow 即可实现 hosts 之间 pingall 通过。

使用方法：
ryu-manager --observe-links simple_spanning_tree_13.py
sudo mn --custom ~/mininet/mininet/MininetTopologies/Simple4S4H.py --topo mytopo --mac --switch ovsk --controller remote
'''
# 用一样的代码，只是 ofproto 为 1.2 则有问题。尚未了解为什么。Anyway, 用最新的总归是好些。


from ryu.base import app_manager
from ryu.controller import ofp_event
from ryu.controller.handler import CONFIG_DISPATCHER, MAIN_DISPATCHER
from ryu.controller.handler import set_ev_cls
from ryu.ofproto import ofproto_v1_3
from ryu.lib.packet import packet
from ryu.lib.packet import ethernet
from ryu.lib.packet import ether_types
from ryu.topology import event, switches
from ryu.topology.api import get_switch, get_link
import thread
import time

class SimpleSpanningTree(app_manager.RyuApp):
    OFP_VERSIONS = [ofproto_v1_3.OFP_VERSION]

    def __init__(self, *args, **kwargs):
        super(SimpleSpanningTree, self).__init__(*args, **kwargs)
        self.mac_to_port = {}
        self.topology_api_app = self
        self.switch_list = []
        self.mSwitches   = []
        self.mDataPaths  = []
        self.links_list  = []
        self.links       = []
        self.ports_to_block  = []
        self.ports_to_enable = []
        self.ev_count = 0
        self.topo_stable = False
        self.start_learning = False


    def delete_flow(self, dpid):
        flag = 1
        i = -1
        while i < len(self.mDataPaths):
            i += 1
            if self.mDataPaths[i].id == dpid:
                flag = 0
                break
        if flag:
            return
        datapath = self.mDataPaths[i]
        ofproto = datapath.ofproto

        match = datapath.ofproto_parser.OFPMatch()

        mod = datapath.ofproto_parser.OFPFlowMod(
            datapath=datapath, match=match, cookie=0,
            command=ofproto.OFPFC_DELETE)
        print "Deleting all flows in ", datapath.id
        datapath.send_msg(mod)

    def block_port(self, dpid, port):
        flag = 1
        i = -1
        while i < len(self.mDataPaths):
            i += 1
            if self.mDataPaths[i].id == dpid:
                flag = 0
                break
        if flag:
            return
        datapath = self.mDataPaths[i]
        ofproto = datapath.ofproto

        match = datapath.ofproto_parser.OFPMatch(in_port=port)
        actions = []
        print 'Blocking packets from switch ', datapath.id,  ', port ', port
        self.add_flow(datapath, 1, match, actions)

    def enbale_port(self, dp1, port1, mac1,  dp2, port2, mac2):
        ofproto = dp1.ofproto
        match = dp1.ofproto_parser.OFPMatch(dst_mac=mac2)
        pass

    @set_ev_cls(event.EventSwitchEnter)
    def stp_schedule(self, ev):
        self.ev_count += 1
        print 'Switch Enter !!!'
        count = self.ev_count

        def go(count):
            time.sleep(5)
            print 'count = %s, ev_count = %s', count, self.ev_count
            if count == self.ev_count:
                print 'I m OK!!!'
                self.topo_stable = True
                self.get_topology_data()

        try:
            thread.start_new_thread(go, (count,))
        except:
            print 'Problem with the threading'

    def get_topology_data(self):
        if not self.topo_stable:
            return
        self.topo_stable = False
        print 'get_topoloty_data'
        self.switch_list = get_switch(self.topology_api_app, None)
        self.mSwitches   = [switch.dp.id for switch in self.switch_list] # switch.dp.id
        self.mDataPaths  = [switch.dp for switch in self.switch_list]
        print type(self.mDataPaths[0])
        self.links_list = get_link(self.topology_api_app, None)
        self.links = [(1, link.src.dpid, link.dst.dpid, link.src.port_no, link.dst.port_no) for link in self.links_list]
        self.links.sort()
        # print 'links       : ', self.links
        print '\n\nlinks:'
        for lk in self.links:
            print 'switch ', lk[1], ', port ', lk[3],  '--> switch ', lk[2], ', port', lk[4]
        print 'switches    : ', self.mSwitches
        self.constructing_stp_krustal()

        # Delete all flows in all datapaths
        for dpid in self.mSwitches:
            self.delete_flow(dpid)
        # Install new flows
        for block in self.ports_to_block:
            if block in self.ports_to_enable:
                continue
            dpid = block[0]
            port = block[1]
            self.block_port(dpid, port)
            # for enable in self.ports_to_enable:
            #     pass
        self.start_learning = True

    def constructing_stp_krustal(self):
        mTopology = {
            'switches':self.mSwitches,
            'links'   :self.links
        }
        # print 'mTopology: \n', mTopology
        parent = dict()
        rank   = dict()

        def make_set(v):
            parent[v] = v
            rank[v]   = 0

        def find(v):
            if parent[v] != v:
                parent[v] = find(parent[v])
            return parent[v]

        def union(v1, v2):
            parent1 = find(v1)
            parent2 = find(v2)
            if parent1 != parent2:
                if rank[parent1] > rank[parent2]:
                    parent[parent2] = parent1
                else:
                    parent[parent1] = parent2
                    if rank[parent2] == rank[parent2]: rank[parent2] += 1

        def kruskal():
            for v in mTopology['switches']:
                make_set(v)
            minimum_spanning_tree = set()
            links = list(mTopology['links'])
            print 'number of links: ', len(links)
            links.sort()
            for link in links:
                weight, switch1, switch2, port1, port2 = link
                if find(switch1) != find(switch2):
                    union(switch1, switch2)
                    minimum_spanning_tree.add(link)
                    if (switch1, port1) not in self.ports_to_enable :
                        self.ports_to_enable.append((switch1, port1))
                    if (switch2, port2) not in self.ports_to_enable:
                        self.ports_to_enable.append((switch2, port2))
                else:
                    if (switch1, port1) not in self.ports_to_block:
                        self.ports_to_block.append((switch1, port1))
                    if (switch2, port2) not in self.ports_to_block:
                        self.ports_to_block.append((switch2, port2))
            print "ports_to_block :         ", self.ports_to_block
            print "ports_to_enable:         ", self.ports_to_enable
            # print "minimum_spanning_tree:  ", list(minimum_spanning_tree)
            return minimum_spanning_tree
        kruskal()

    @set_ev_cls(ofp_event.EventOFPSwitchFeatures, CONFIG_DISPATCHER)
    def switch_features_handler(self, ev):
        datapath = ev.msg.datapath
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser

        # install table-miss flow entry
        #
        # We specify NO BUFFER to max_len of the output action due to
        # OVS bug. At this moment, if we specify a lesser number, e.g.,
        # 128, OVS will send Packet-In with invalid buffer_id and
        # truncated packet data. In that case, we cannot output packets
        # correctly.  The bug has been fixed in OVS v2.1.0.
        match = parser.OFPMatch()
        actions = [parser.OFPActionOutput(ofproto.OFPP_CONTROLLER,
                                          ofproto.OFPCML_NO_BUFFER)]
        self.add_flow(datapath, 0, match, actions)

    def add_flow(self, datapath, priority, match, actions, buffer_id=None):
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser

        inst = [parser.OFPInstructionActions(ofproto.OFPIT_APPLY_ACTIONS,
                                             actions)]
        if buffer_id:
            mod = parser.OFPFlowMod(datapath=datapath, buffer_id=buffer_id,
                                    priority=priority, match=match,
                                    instructions=inst)
        else:
            mod = parser.OFPFlowMod(datapath=datapath, priority=priority,
                                    match=match, instructions=inst)
        datapath.send_msg(mod)

    @set_ev_cls(ofp_event.EventOFPPacketIn, MAIN_DISPATCHER)
    def _packet_in_handler(self, ev):
        if not self.start_learning:
            return

        # If you hit this you might want to increase
        # the "miss_send_length" of your switch
        if ev.msg.msg_len < ev.msg.total_len:
            self.logger.debug("packet truncated: only %s of %s bytes",
                              ev.msg.msg_len, ev.msg.total_len)
        msg = ev.msg
        datapath = msg.datapath
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser
        in_port = msg.match['in_port']

        pkt = packet.Packet(msg.data)
        eth = pkt.get_protocols(ethernet.ethernet)[0]

        if eth.ethertype == ether_types.ETH_TYPE_LLDP:
            # ignore lldp packet
            return
        dst = eth.dst
        src = eth.src

        dpid = datapath.id
        self.mac_to_port.setdefault(dpid, {})

        self.logger.info("packet in %s %s %s %s", dpid, src, dst, in_port)

        # learn a mac address to avoid FLOOD next time.
        self.mac_to_port[dpid][src] = in_port

        if dst in self.mac_to_port[dpid]:
            out_port = self.mac_to_port[dpid][dst]
        else:
            out_port = ofproto.OFPP_FLOOD

        actions = [parser.OFPActionOutput(out_port)]

        # install a flow to avoid packet_in next time
        if out_port != ofproto.OFPP_FLOOD:
            print "installing flow to avoid packet_in next time"
            match = parser.OFPMatch(in_port=in_port, eth_dst=dst)
            # verify if we have a valid buffer_id, if yes avoid to send both
            # flow_mod & packet_out
            if msg.buffer_id != ofproto.OFP_NO_BUFFER:
                self.add_flow(datapath, 1, match, actions, msg.buffer_id)
                return
            else:
                self.add_flow(datapath, 1, match, actions)
        data = None
        if msg.buffer_id == ofproto.OFP_NO_BUFFER:
            data = msg.data

        out = parser.OFPPacketOut(datapath=datapath, buffer_id=msg.buffer_id,
                                  in_port=in_port, actions=actions, data=data)
        datapath.send_msg(out)
