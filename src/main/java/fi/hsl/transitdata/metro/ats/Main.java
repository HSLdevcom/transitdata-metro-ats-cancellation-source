package fi.hsl.transitdata.metro.ats;

import com.typesafe.config.Config;
import fi.hsl.common.config.ConfigParser;
import fi.hsl.common.pulsar.PulsarApplication;
import fi.hsl.common.pulsar.PulsarApplicationContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main {

    private static final Logger log = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) {
        log.info("Starting transitdata-metro-ats-cancellation-source");
        final Config config = ConfigParser.createConfig();

        try (final PulsarApplication app = PulsarApplication.newInstance(config)) {
            final PulsarApplicationContext context = app.getContext();
            final MetroCancellationFactory metroCancellationFactory = new MetroCancellationFactory();
            final MessageHandler handler = new MessageHandler(context, metroCancellationFactory);
            log.info("Start handling the messages");
            app.launchWithHandler(handler);
        } catch (Exception e) {
            log.error("Exception at main", e);
        }
    }
}
