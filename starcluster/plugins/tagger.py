# https://cheminsilico.blogspot.kr/2014/11/tag-your-aws-resources-in-starcluster.html
# Install this file to $HOME/.starcluster/plugins/tagger.py or somewhere on your $PYTHONPATH
from starcluster.clustersetup import ClusterSetup
from starcluster.logger import log

class TaggerPlugin(ClusterSetup):
    def __init__(self, tags):
        self.tags = eval(tags)

    def set_volume_tags(self, node):
        volumes = [v for v in node.instance.connection.get_all_volumes() if v.attach_data.instance_id == node.instance.id]
        for volume in volumes:
	    log.info("Applying tags to %s" % node)
            volume.tags.update(self.tags)

    def run(self, nodes, master, user, user_shell, volumes):
        log.info("Tagging all nodes...")
	for tag in self.tags:
	    val = self.tags.get(tag)
	    log.info("Applying tag - {0}: {1}".format(tag, val))
	    for node in nodes:
		node.add_tag(tag, val)
        for node in nodes:
            self.set_volume_tags(node)

    def on_add_node(self, node, nodes, master, user, user_shell, volumes):
        log.info("Tagging new node...")
	for tag in self.tags:
	    val = self.tags.get(tag)
	    log.info("Applying tag - {0}: {1}".format(tag, val))
	    node.add_tag(tag, val)
        self.set_volume_tags(node)

