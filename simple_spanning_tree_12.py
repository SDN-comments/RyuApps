__author__ = 'mk'
# Run the application from command line:
# ryu-manager --observe-links simple_spanning_tree_12.py

import logging
import struct

from ryu.base import app_manager
from ryu.controller import ofp_event
from ryu.controller.handler import MAIN_DISPATCHER
from ryu.controller.handler import set_ev_cls
from ryu.ofproto import ofproto_v1_2
from ryu.lib.packet import packet
from ryu.lib.packet import ethernet
from ryu.lib.packet import ether_types
from ryu.topology import event, switches
from ryu.topology.api import get_switch, get_link


class SimpleSwitch12(app_manager.RyuApp):
    OFP_VERSIONS = [ofproto_v1_2.OFP_VERSION]

    def __init__(self, *args, **kwargs):
        super(SimpleSwitch12, self).__init__(*args, **kwargs)
        self.mac_to_port = {}
        self.topology_api_app = self
        self.switch_list = []
        self.mSwitches   = []
        self.mDataPaths  = []
        self.links_list  = []
        self.links       = []
        self.ports_to_block  = []
        self.ports_to_enable = []

    def delete_flow(self, dpid):
        flag = 1
        i = 0
        while i < len(self.mDataPaths):
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
        print "Deleting all flows in %s", datapath
        datapath.send_msg(mod)

    def block_port(self, dpid, port):
        flag = 1
        i = 0
        while i < len(self.mDataPaths):
            if self.mDataPaths[i].id == dpid:
                flag = 0
                break
        if flag:
            return
        datapath = self.mDataPaths[i]
        ofproto = datapath.ofproto

        match = datapath.ofproto_parser.OFPMatch(in_port=port)
        actions = []
        inst = [datapath.ofproto_parser.OFPInstructionActions(
            ofproto.OFPIT_APPLY_ACTIONS, actions)]
        mod = datapath.ofproto_parser.OFPFlowMod(
            datapath=datapath, cookie=0, cookie_mask=0, table_id=0,
            command=ofproto.OFPFC_ADD, idle_timeout=0, hard_timeout=0,
            priority=0, buffer_id=ofproto.OFP_NO_BUFFER,
            out_port=ofproto.OFPP_ANY,
            out_group=ofproto.OFPG_ANY,
            flags=0, match=match, instructions=inst)
        datapath.send_msg(mod)
        print 'Blocking switch %s, port %s', datapath, port

    def enbale_port(self, dp1, port1, mac1,  dp2, port2, mac2):
        ofproto = dp1.ofproto
        match = dp1.ofproto_parser.OFPMatch(dst_mac=mac2)
        pass

    @set_ev_cls(event.EventSwitchEnter)
    def get_topology_data(self, ev):
        self.switch_list = get_switch(self.topology_api_app, None)
        self.mSwitches   = [switch.dp.id for switch in self.switch_list] # switch.dp.id
        self.mDataPaths  = [switch.dp for switch in self.switch_list]
        #print type(self.mDataPaths[0])
        #print type(self.mDataPaths[0].dp)
        #print type(self.mDataPaths[0].dp.id)
        self.links_list = get_link(self.topology_api_app, None)
        self.links = [(1, link.src.dpid, link.dst.dpid, link.src.port_no, link.dst.port_no) for link in self.links_list]
        print 'links       : ', self.links
        print 'switches    : ', self.mSwitches
        self.constructing_stp_krustal()

        # Delete all flows in all datapaths
        for dp in self.mSwitches:
            self.delete_flow(dp)
        # Install new flows
        for block in self.ports_to_block:
            dp = block[0]
            port = block[1]
            self.block_port(dp, port)
        for enable in self.ports_to_enable:
            pass

    def constructing_stp_krustal(self):
        mTopology = {
            'switches':self.mSwitches,
            'links'   :self.links
        }
        print 'mTopology: \n', mTopology
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
            links.sort()
            for link in links:
                weight, switch1, switch2, port1, port2 = link
                if find(switch1) != find(switch2):
                    union(switch1, switch2)
                    minimum_spanning_tree.add(link)
                    if (switch1, port1, switch2, port2) not in self.ports_to_enable \
                            and (switch1, port1, switch2, port2) not in self.ports_to_enable:
                        self.ports_to_enable.append((switch1, port1, switch2, port2))
                else:
                    if (switch1, port1) not in self.ports_to_block:
                        self.ports_to_block.append((switch1, port1))
                    if (switch2, port2) not in self.ports_to_block:
                        self.ports_to_block.append((switch2, port1))
            print "ports_to_block :         ", self.ports_to_block
            print "ports_to_enable:         ", self.ports_to_enable
            print "minimum_spanning_tree:  ", list(minimum_spanning_tree)
            return minimum_spanning_tree
        kruskal()


    def add_flow(self, datapath, port, dst, actions):
        ofproto = datapath.ofproto

        match = datapath.ofproto_parser.OFPMatch(in_port=port,
                                                 eth_dst=dst)
        inst = [datapath.ofproto_parser.OFPInstructionActions(
            ofproto.OFPIT_APPLY_ACTIONS, actions)]

        mod = datapath.ofproto_parser.OFPFlowMod(
            datapath=datapath, cookie=0, cookie_mask=0, table_id=0,
            command=ofproto.OFPFC_ADD, idle_timeout=0, hard_timeout=0,
            priority=0, buffer_id=ofproto.OFP_NO_BUFFER,
            out_port=ofproto.OFPP_ANY,
            out_group=ofproto.OFPG_ANY,
            flags=0, match=match, instructions=inst)
        datapath.send_msg(mod)

    @set_ev_cls(ofp_event.EventOFPPacketIn, MAIN_DISPATCHER)
    def _packet_in_handler(self, ev):
        msg = ev.msg
        datapath = msg.datapath
        ofproto = datapath.ofproto
        in_port = msg.match['in_port']

        pkt = packet.Packet(msg.data)
        eth = pkt.get_protocols(ethernet.ethernet)[0]

        dst = eth.dst
        src = eth.src

        dpid = datapath.id
        self.mac_to_port.setdefault(dpid, {})

        # self.logger.info("packet in %s %s %s %s", dpid, src, dst, in_port)

        # learn a mac address to avoid FLOOD next time.
        self.mac_to_port[dpid][src] = in_port

        if dst in self.mac_to_port[dpid]:
            out_port = self.mac_to_port[dpid][dst]
        else:
            out_port = ofproto.OFPP_FLOOD

        actions = [datapath.ofproto_parser.OFPActionOutput(out_port)]

        # install a flow to avoid packet_in next time
        if out_port != ofproto.OFPP_FLOOD:
            self.add_flow(datapath, in_port, dst, actions)

        data = None
        if msg.buffer_id == ofproto.OFP_NO_BUFFER:
            data = msg.data

        out = datapath.ofproto_parser.OFPPacketOut(
            datapath=datapath, buffer_id=msg.buffer_id, in_port=in_port,
            actions=actions, data=data)
        datapath.send_msg(out)


