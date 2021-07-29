package māia.topology.node.standard.ml.learner

import māia.configure.Configurable
import māia.configure.asReconfigureBlock
import māia.ml.dataset.DataBatch
import māia.ml.dataset.DataRow
import māia.ml.dataset.DataStream
import māia.ml.dataset.WithColumnHeaders
import māia.ml.learner.Learner
import māia.topology.Node
import māia.topology.NodeConfiguration
import māia.topology.io.Input
import māia.topology.io.Output
import māia.topology.io.Throughput
import māia.topology.io.util.allClosed
import māia.topology.node.base.ContinuousLoopNode


@Node.WithMetadata("Node which handles the life-cycle of a learner")
class LearnerNode : ContinuousLoopNode<LearnerNodeConfiguration> {

    @Configurable.Register<LearnerNode, LearnerNodeConfiguration>(LearnerNode::class, LearnerNodeConfiguration::class)
    constructor(block : LearnerNodeConfiguration.() -> Unit = {}) : super(block)

    constructor(config: LearnerNodeConfiguration) : this(config.asReconfigureBlock())

    @Throughput.WithMetadata("Supplies a learner to the node")
    val learnerInput by Input<Learner<*>>()

    @Throughput.WithMetadata("Initialises the learner")
    val initialise by Input<WithColumnHeaders>()

    @Throughput.WithMetadata("Provides a data-set to train the learner on")
    val train by Input<DataStream<*>>()

    @Throughput.WithMetadata("Input taking rows to make predictions from")
    val predictionInput by Input<DataStream<*>>()

    @Throughput.WithMetadata(
            "Outputs the learner in its current state " +
            "each time this input receives any value")
    val pushLearner by Input<Any?>()

    @Throughput.WithMetadata("Supplies the current state of the learner")
    val learner by Output<Learner<*>>()

    @Throughput.WithMetadata("Outputs the results of predictions")
    val predictionOutput by Output<Pair<DataRow, DataRow>>()

    private val outputs by lazy { Pair(predictionOutput, learner) }

    private lateinit var learnerInstance : Learner<*>

    private lateinit var inputs : List<Input<*>>

    override fun loopCondition() : Boolean = !outputs.allClosed

    override suspend fun preLoop() {
        // Receive the learner
        learnerInstance = learnerInput.pullOrAbort()

        // Gather the train and classify inputs into a selectable collection
        inputs = inputListForLearner(learnerInstance)
    }

    override suspend fun mainLoopInner() {
        inputs.selectOrAbort { input, value ->
            when (input) {
                pushLearner -> learner.push(learnerInstance)
                learnerInput -> {
                    learnerInstance = value as Learner<*>
                    inputs = inputListForLearner(learnerInstance)
                }
                initialise -> {
                    learnerInstance.initialise(value as WithColumnHeaders)
                    inputs = inputListForLearner(learnerInstance)
                }
                train -> {
                    if (learnerInstance.isIncremental)
                        (learnerInstance as Learner<DataStream<*>>).train(value as DataStream<*>)
                    else
                        (learnerInstance as Learner<DataBatch<*, *>>).train(value as DataBatch<*, *>)
                }
                else -> {
                    for (row in (value as DataStream<*>).rowIterator()) {
                        if (predictionOutput.isClosed) break
                        predictionOutput.push(Pair(row, learnerInstance.predict(row)))
                    }
                }
            }
        }
    }

    override suspend fun postLoop() {
        learner.push(learnerInstance)
    }

    private fun inputListForLearner(learner : Learner<*>) : List<Input<*>> {
        return if (learner.isInitialised)
            listOf(train, predictionInput, pushLearner, learnerInput, initialise)
        else
            listOf(pushLearner, learnerInput, initialise)
    }

}

class LearnerNodeConfiguration : NodeConfiguration("learnerNode") {

}
