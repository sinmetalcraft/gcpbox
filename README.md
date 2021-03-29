# gcpbox

Google Cloud Platform Utility

[@sinmetal](https://github.com/sinmetal) が GCP でよく利用する処理をまとめている Utility。
[@sinmetal](https://github.com/sinmetal) のユースケースに特化しているので、他の人が使うことは気にしていないが、参考程度に見るのはいいかもしれない。

## cloudtasks

* Handlerの [Header](https://cloud.google.com/tasks/docs/creating-appengine-handlers?hl=en#reading_app_engine_task_request_headers) を取得
* TaskをAdd

## metadata

GCPのmetadata周りを扱うutility

GCP上では [Metadata Server](https://cloud.google.com/compute/docs/storing-retrieving-metadata) を参照し、Localでは環境変数を利用し、設定値を取得する。
GCPとLocalで同じロジックを使いつつ、設定値を持ち回るために作成した

### 動作確認した環境

* Google App Engine Standard for Go 1.15
* Google Compute Engine
* Google Kubernetes Engine

## storage

* Create [SignedURL](https://cloud.google.com/storage/docs/access-control/signed-urls?hl=en)
* [PubSubNotify](https://cloud.google.com/storage/docs/pubsub-notifications?hl=en) Body Reader
