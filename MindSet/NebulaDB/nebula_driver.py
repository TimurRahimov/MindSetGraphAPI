from nebula3.Config import Config
from nebula3.gclient.net import ConnectionPool
from nebula3.mclient import MetaCache
from nebula3.sclient.GraphStorageClient import GraphStorageClient

import networkx as nx
import pandas as pd
import re
import time


def create_nx_graph(vertices, edges):
    G = nx.Graph()

    for fullname in vertices:
        G.add_node(fullname)

    for edge in edges:
        G.add_edge(edge[0], edge[1], title=edge[2])

    return G


def generate_graphml(G: nx.Graph) -> str:
    gml_generator = nx.generate_graphml(G)
    gml_string = "\n".join(gml_generator)
    utf_decimal_regex = re.compile(r"\&\#(\d\d\d\d);")

    gml_string = utf_decimal_regex.sub(
        lambda match: f"{chr(int(match.groups()[0]))}",
        gml_string
    )

    return gml_string


class NebulaDriver:
    __config = Config()
    __config.max_connection_pool_size = 2
    __connection_pool = ConnectionPool()
    __connection_pool.init([('127.0.0.1', 9669)], __config)
    __meta_cache = MetaCache([('127.0.0.1', 9559)], 50000)

    gclient = __connection_pool.get_session('root', 'nebula')
    sclient = GraphStorageClient(__meta_cache)

    @classmethod
    def create_objects_in_database(cls):
        resp = cls.gclient.execute(
            'CREATE SPACE IF NOT EXISTS eventRelations(vid_type=FIXED_STRING(255));'
            'USE eventRelations;'
            'CREATE TAG IF NOT EXISTS person(fullname FIXED_STRING(255));'
            'CREATE EDGE IF NOT EXISTS event(event_id INT64)'
        )

        if not resp.is_succeeded():
            return {
                "NebulaError": resp.error_msg()
            }

        time.sleep(10)
        return {
            "NebulaSuccess": "Space, tag and edge created successfully"
        }

    @classmethod
    def drop_space(cls):
        resp = cls.gclient.execute(
            'DROP SPACE eventRelations;'
        )

        if not resp.is_succeeded():
            return {
                "NebulaError": resp.error_msg()
            }

        return {
            "NebulaSuccess": "Space dropped successfully"
        }

    @classmethod
    def get_all_vertices(cls) -> list:

        vertices = list()

        resp = cls.sclient.scan_vertex(
            space_name='eventRelations',
            tag_name='person')

        while resp.has_next():
            result = resp.next()
            if result is None:
                break
            for vertex_data in result:
                node = vertex_data.as_node()
                fullname = node.get_id().as_string()
                vertices.append(fullname)

        return vertices

    @classmethod
    def get_all_edges(cls) -> list:

        edges = list()

        resp = cls.sclient.scan_edge(
            space_name='eventRelations',
            edge_name='event'
        )

        while resp.has_next():
            result = resp.next()
            if result is None:
                break
            for edge_data in result:
                relationship = edge_data.as_relationship()

                human_first = str(relationship.start_vertex_id().as_string())
                human_second = str(relationship.end_vertex_id().as_string())
                event_id = str(edge_data.get_prop_values()[0])

                edges.append([human_first, human_second, event_id])

        return edges

    @classmethod
    def get_all_in_graphml(cls) -> str:
        G = create_nx_graph(cls.get_all_vertices(), cls.get_all_edges())
        return generate_graphml(G)

    @classmethod
    def get_subgraph(cls, vid_from: str, view: str):
        resp = cls.gclient.execute(
            f"USE eventRelations;"
            f"GET SUBGRAPH WITH PROP 1000 STEPS FROM \"{vid_from}\" "
            f"YIELD VERTICES AS nodes, EDGES AS relationships;"
        )

        if not resp.is_succeeded():
            return {
                "NebulaError": resp.error_msg()
            }

        print(resp.is_succeeded())
        print(resp.error_msg())

        vertices = [
            person.as_node().get_id().as_string()
            for node in resp.column_values('nodes')
            for person in node.as_list()
        ]

        edges = [
            [
                event.start_vertex_id().as_string(),
                event.end_vertex_id().as_string(),
                event.properties()['event_id'].as_int()
            ]
            for relation in resp.column_values('relationships')
            for event in [e.as_relationship() for e in relation.as_list()]
        ]

        if view == 'graphml':
            G = create_nx_graph(vertices, edges)
            graphml_string = generate_graphml(G)
            return graphml_string

        return {
            'nodes': vertices,
            'relationships': edges
        }

    @classmethod
    def insert_data(cls, data: pd.DataFrame, limit=-1):
        count = 0

        if limit != -1:
            data = data.sample(frac=1).reset_index(drop=True)

        for row_index, row in data.iterrows():
            count += 1

            if limit != -1 and count >= limit:
                break

            event_id = row[0]
            person_first = row[1]
            person_second = row[2]

            cmd = \
                ('USE eventRelations;'
                 'INSERT VERTEX person(fullname) VALUES \"{}\":(\"{}\");'
                 'INSERT VERTEX person(fullname) VALUES \"{}\":(\"{}\");'
                 'INSERT EDGE event(event_id) VALUES \"{}\"->\"{}\":({});'
                 ).format(
                    *[person_first] * 2,
                    *[person_second] * 2,
                    person_first, person_second, event_id
                )

            resp = cls.gclient.execute(cmd)

            if not resp.is_succeeded():
                return {
                    "NebulaError": resp.error_msg()
                }

        return {
            "NebulaSuccess": f"{count}, rows inserted successfully"
        }
