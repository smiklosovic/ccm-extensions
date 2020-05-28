import argparse  # TODO: remove dep
import logging
import shutil
from pathlib import Path
from typing import Union, NamedTuple, List

import cassandra.connection
import cassandra.cluster
import pkg_resources
import yaml
from ccmlib.cluster import Cluster
from ccmlib.dse_cluster import DseCluster
from ccmlib.node import Node

# TODO: not sure if module-level is the right place for this
for entry_point in pkg_resources.iter_entry_points(group='ccm_extension'):
    entry_point.load()()

def basic_topology(data_centers=1, racks=1, nodes=1):
    def make_racks():
        return {f'rack{rack}': nodes for rack in range(racks)}

    return {f'dc{dc}': make_racks() for dc in range(data_centers)}


def _expand_topology(topology: {str: {str, int}}):
    for dc, racks in topology.items():
        for rack, count in racks.items():
            for x in range(count):
                yield dc, rack


class JvmDebugOptions(NamedTuple):
    wait_for_debugger: bool
    address: str


# TODO: move somewhere else?
def ExistingFileArgType(path: Union[str, Path]):
    path = Path(path)

    if not path.exists():
        raise argparse.ArgumentTypeError(f'file "{path}" does not exist.')

    if not path.is_file():
        raise argparse.ArgumentTypeError(f'"{path}" is not a regular file.')

    return path


class CqlSchema(NamedTuple):
    path: Path
    statements: List[str]

    @staticmethod
    def ArgType(arg: str):
        path = ExistingFileArgType(arg)

        return CqlSchema.from_path(path)

    @staticmethod
    def from_path(path: Path):
        with path.open('r') as f:
            schema = yaml.load(f, Loader=yaml.SafeLoader)

            if not isinstance(schema, list):
                raise argparse.ArgumentTypeError(f'root of the schema YAML must be a list. Got a {type(schema).__name__}.')

            for i, o in enumerate(schema):
                if not isinstance(o, str):
                    raise argparse.ArgumentTypeError(f'schema YAML must be a list of statement strings. Item {i} is a {type(o).__name__}.')

            return CqlSchema(path, schema)

    @staticmethod
    def default_schema_path():
        return "schema.yaml"

    @staticmethod
    def add_schema_argument(name, parser):
        parser.add_argument(name, type=CqlSchema.ArgType,
                            help="CQL schema file to apply (default: %(default)s)",
                            default=str(CqlSchema.default_schema_path()))

def _ExtendedCluster(superclass):
    class ExtendedClusterImpl(superclass):
        logger = logging.getLogger(f'{__name__}.{__qualname__}')

        def __init__(self, cluster_directory: Path, cassandra_version: str, topology: {str: {str, int}},
                     delete_cluster_on_stop: bool = True, vnodes: Union[bool, int] = 16, populate=True, ipformat = None, **kwargs):

            if cluster_directory.exists():
                cluster_directory.rmdir()  # CCM wants to create this

            super().__init__(
                path=cluster_directory.parent,
                name=cluster_directory.name,
                version=cassandra_version,
                create_directory=True,  # if this is false, various config files wont be created...,
                **kwargs
            )

            self.delete_cluster_on_stop = delete_cluster_on_stop

            if populate:
                self.populate(topology, vnodes, ipformat=ipformat)

        def populate(self, topology: {str: {str, int}}, vnodes: Union[bool, int] = False,
                     jvm_debug_options: JvmDebugOptions = None, ipformat = None, **kwargs):
            topo = list(_expand_topology(topology))

            result = super().populate(len(topo), use_vnodes=vnodes is not False, ipformat=ipformat, **kwargs)

            for i, ((dc, rack), node) in enumerate(zip(topo, self.nodelist())):
                node.dc, node.rack = dc, rack

                if self.use_vnodes:
                    node.set_configuration_options({
                        'num_tokens': int(vnodes)
                    })

                self.configure_node(node, i)

            return result

        def configure_node(self, node: Node, index):
            # set dc/rack manually, since CCM doesn't support custom racks
            node.set_configuration_options({
                'endpoint_snitch': 'GossipingPropertyFileSnitch',
            })

            rackdc_path = Path(node.get_conf_dir()) / 'cassandra-rackdc.properties'

            with open(rackdc_path, 'w') as f:
                f.write(f'dc={node.dc}\nrack={node.rack}\n')

        def stop(self, **kwargs):
            result = super().stop(**kwargs)

            if self.delete_cluster_on_stop:
                shutil.rmtree(self.get_path())

            return result

        def apply_schema(self, schema: CqlSchema):
            contact_points = map(lambda n: cassandra.connection.DefaultEndPoint(*n.network_interfaces['binary']), self.nodelist())

            with cassandra.cluster.Cluster(list(contact_points)) as cql_cluster:
                with cql_cluster.connect() as cql_session:
                    for stmt in schema.statements:
                        self.logger.debug('Executing CQL statement "{}".'.format(stmt.split('\n')[0]))
                        cql_session.execute(stmt)

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc_val, exc_tb):
            self.stop()

    return ExtendedClusterImpl

ExtendedCluster = _ExtendedCluster(Cluster)
ExtendedDseCluster = _ExtendedCluster(DseCluster)
