__author__ = 'francklin and chenlunde'

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

class SimpleSpanningTree(app_manager.RyuApp):
    OFP_VERSIONS = [ofproto_v1_3.OFP_VERSION]

    def __init__(self, *args, **kwargs):
        super(SimpleSpanningTree, self).__init__(*args, **kwargs)
        self.mac_to_port = {}
        self.topology_api_app = self

        self.switch_list = []
        self.switches    = []
        self.links_list  = []
        self.links       = []
        self.ports_to_block = {}

    @set_ev_cls(event.EventSwitchEnter)
    def get_topology_data(self, ev):
        self.switch_list = get_switch(self.topology_api_app, None)
        self.switches = [switch.dp.id for switch in self.switch_list]
        self.links_list = get_link(self.topology_api_app, None)
        self.links = [(1, link.src.dpid, link.dst.dpid, link.src.port_no, link.dst.port_no) for link in self.links_list]
        print 'links   : ', self.links
        print 'switches: ', self.switches
        self.constructing_stp_krustal()

    def constructing_stp_krustal(self):
        mTopology = {
            'switches':self.switches,
            'links'   :self.links
                     }
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
                else:
                    if switch1 in self.ports_to_block:
                        self.ports_to_block[switch1].append(port1)
                    else:
                        self.ports_to_block[switch1] = [port1]
                    if switch2 in self.ports_to_block:
                        self.ports_to_block[switch2].append(port2)
                    else:
                        self.ports_to_block[switch2] = [port2]
            print "ports_to_block: ", self.ports_to_block
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


