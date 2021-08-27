package example

import example.ApiError._
import sttp.client3.asynchttpclient.zio.{ SttpClient, send }
import sttp.client3.ziojson.asJson
import sttp.client3.{ UriContext, basicRequest }
import sttp.model.Uri

import zio._
import zio.duration._
import zio.json._
import zio.redis._

object ContributorsCache {
  trait Service {
    def fetchAll(repository: Repository): IO[ApiError, Contributors]
  }

  lazy val live: ZLayer[RedisExecutor with SttpClient, Nothing, ContributorsCache] =
    ZLayer.fromFunction { env =>
      new Service {
        def fetchAll(repository: Repository): IO[ApiError, Contributors] =
          (read(repository) <> retrieve(repository)).provide(env)
      }
    }

  private[this] def read(repository: Repository): ZIO[RedisExecutor, ApiError, Contributors] =
    get(repository.key)
      .returning[String]
      .someOrFail(ApiError.CacheMiss(repository.key))
      .map(_.fromJson[Contributors])
      .rightOrFail(ApiError.CorruptedData)
      .refineToOrDie[ApiError]

  private[this] def retrieve(repository: Repository): ZIO[RedisExecutor with SttpClient, ApiError, Contributors] =
    for {
      req          <- ZIO.succeed(basicRequest.get(urlOf(repository)).response(asJson[Chunk[Contributor]]))
      res          <- send(req).orElseFail(GithubUnreachable)
      contributors <- res.body.fold(_ => ZIO.fail(UnknownProject(urlOf(repository).toString)), ZIO.succeed(_))
      _            <- cache(repository, contributors)
    } yield Contributors(contributors)

  private def cache(repository: Repository, contributors: Chunk[Contributor]): URIO[RedisExecutor, Any] =
    ZIO
      .fromOption(NonEmptyChunk.fromChunk(contributors))
      .map(Contributors(_).toJson)
      .flatMap(data => set(repository.key, data, Some(1.minute)).orDie)
      .ignore

  private[this] def urlOf(repository: Repository): Uri =
    uri"https://api.github.com/repos/${repository.owner}/${repository.name}/contributors"
}
