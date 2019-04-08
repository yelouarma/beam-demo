package ma.octo.demo;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;

public class Vision360PipeLine {


    static class PrintElement extends DoFn<KV<String, CoGbkResult>, String> {

        @ProcessElement
        public void processElement(ProcessContext c) {
            System.out.println(c.element());
        }
    }

    public static void main(String[] args) {

        Pipeline p = Pipeline.create();
        PCollection<KV<String, String>> users = p.apply(TextIO.read().from("/Users/yelouarma/Documents/workspace/beam/src/main/resources/USERS.csv"))
                .apply(WithKeys.of(new SerializableFunction<String, String>() {
                    public String apply(String s) {
                        return s.split(",")[3];
                    }
                }));

        PCollection<KV<String, String>> cards = p.apply(TextIO.read().from("/Users/yelouarma/Documents/workspace/beam/src/main/resources/CARDS.csv"))
                .apply(WithKeys.of(new SerializableFunction<String, String>() {
                    public String apply(String s) {
                        return s.split(",")[1];
                    }
                }));


        final TupleTag<String> userTag = new TupleTag<String>();
        final TupleTag<String> cardTag = new TupleTag<String>();

        PCollection<KV<String, CoGbkResult>> joinedCollection =
                KeyedPCollectionTuple.of(userTag, users)
                        .and(cardTag, cards)
                        .apply(CoGroupByKey.<String>create());


        joinedCollection.apply(ParDo.of(new PrintElement()));

        p.run().waitUntilFinish();
    }
}
