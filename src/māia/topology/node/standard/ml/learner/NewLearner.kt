package māia.topology.node.standard.ml.learner

import māia.configure.Configurable
import māia.configure.Configuration
import māia.configure.ConfigurationElement
import māia.configure.ConfigurationItem
import māia.configure.SubConfiguration
import māia.configure.asReconfigureBlock
import māia.configure.getConfigurationClassUntyped
import māia.configure.getConfigurationObjectConstructorUntyped
import māia.configure.util.classMatchesConfiguration
import māia.configure.util.ifNotAbsent
import māia.ml.learner.Learner
import māia.ml.learner.factory.ConfigurableLearnerFactory
import māia.topology.ExecutionState
import māia.topology.Node
import māia.topology.NodeConfiguration
import māia.topology.node.base.Source
import kotlin.reflect.KClass

@Node.WithMetadata("Creates instances of a learner")
class NewLearner : Source<NewLearnerConfiguration, Learner<*>> {

    @Configurable.Register<NewLearner, NewLearnerConfiguration>(NewLearner::class, NewLearnerConfiguration::class)
    constructor(block : NewLearnerConfiguration.() -> Unit = {}) : super(block)

    constructor(config : NewLearnerConfiguration) : this(config.asReconfigureBlock())

    var times by ExecutionState { 0 }

    val factory : ConfigurableLearnerFactory<*, *>
    init {
        val factoryClass = configuration.factoryClass as KClass<out Configurable<*>>
        val constructor = factoryClass.getConfigurationObjectConstructorUntyped(getConfigurationClassUntyped(factoryClass))
        factory = constructor(configuration.learnerConfiguration) as ConfigurableLearnerFactory<*, *>
    }

    override suspend fun produce(): Learner<*> {
        ifNotAbsent {configuration.repeat} then {
            repeat -> if (++times > repeat) stop()
        } otherwise {
            stop()
        }

        return factory.create()
    }

}

class NewLearnerConfiguration : NodeConfiguration("newLearner") {

    @ConfigurationElement.WithMetadata("The type of learner to create")
    var factoryClass by ConfigurationItem<KClass<out ConfigurableLearnerFactory<*, *>>>()

    @ConfigurationElement.WithMetadata("The configuration for the learner")
    var learnerConfiguration by SubConfiguration<Configuration>(Configuration::class)

    @ConfigurationElement.WithMetadata("The number of instances of the learner to output")
    var repeat by ConfigurationItem<Int>(optional = true)

    override fun checkIntegrity() : String? {
        return super.checkIntegrity()
                ?: classMatchesConfiguration(factoryClass, learnerConfiguration)
                ?: ifNotAbsent { repeat } then {
                    if (it < 0)
                        "Value for repeat can't be negative; got $repeat"
                    else
                        null
                } otherwise {
                    null
                }
    }

}
