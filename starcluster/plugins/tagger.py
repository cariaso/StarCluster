# from https://cheminsilico.blogspot.com/2014/11/tag-your-aws-resources-in-starcluster.html
# Install this file to $HOME/.starcluster/plugins/tagger.py or somewhere on your $PYTHONPATH
from starcluster.clustersetup import ClusterSetup
from starcluster.logger import log

class TaggerPlugin(ClusterSetup):
    def __init__(self, tags):
        self.tags = eval(tags)

    def run(self, nodes, master, user, user_shell, volumes):
        log.info("Tagging all nodes...")
	for tag in self.tags:
	    val = self.tags.get(tag)
	    log.info("Applying tag - {0}: {1}".format(tag, val))
	    for node in nodes:
		node.add_tag(tag, val)

    def on_add_node(self, node, nodes, master, user, user_shell, volumes):
        log.info("Tagging new node...")
	for tag in self.tags:
	    val = self.tags.get(tag)
	    log.info("Applying tag - {0}: {1}".format(tag, val))
	    node.add_tag(tag, val)
