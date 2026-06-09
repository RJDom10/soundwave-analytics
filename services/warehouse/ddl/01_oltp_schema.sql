-- Schema OLTP de SoundWave Analytics
-- Este archivo se ejecuta automáticamente al levantar el container de MySQL

CREATE TABLE IF NOT EXISTS artists (
    artist_id     VARCHAR(36)   PRIMARY KEY,
    name          VARCHAR(255)  NOT NULL,
    country       VARCHAR(100),
    genre         VARCHAR(100),
    monthly_listeners INT DEFAULT 0,
    created_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS albums (
    album_id      VARCHAR(36)   PRIMARY KEY,
    title         VARCHAR(500)  NOT NULL,
    artist_id     VARCHAR(36)   NOT NULL,
    release_date  DATE,
    FOREIGN KEY (artist_id) REFERENCES artists(artist_id)
);

CREATE TABLE IF NOT EXISTS tracks (
    track_id      VARCHAR(36)   PRIMARY KEY,
    title         VARCHAR(500)  NOT NULL,
    artist_id     VARCHAR(36)   NOT NULL,
    album_id      VARCHAR(36),
    duration_ms   INT,
    explicit      BOOLEAN DEFAULT FALSE,
    release_date  DATE,
    popularity    TINYINT DEFAULT 0,
    FOREIGN KEY (artist_id) REFERENCES artists(artist_id),
    FOREIGN KEY (album_id)  REFERENCES albums(album_id)
);

CREATE TABLE IF NOT EXISTS users (
    user_id       VARCHAR(36)   PRIMARY KEY,
    username      VARCHAR(100)  UNIQUE NOT NULL,
    country       VARCHAR(50),
    subscription  ENUM('free','premium') DEFAULT 'free',
    registered_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS play_events (
    event_id          BIGINT AUTO_INCREMENT PRIMARY KEY,
    user_id           VARCHAR(36)  NOT NULL,
    track_id          VARCHAR(36)  NOT NULL,
    played_at         TIMESTAMP    NOT NULL,
    duration_played_ms INT,
    source            ENUM('radio','search','playlist','recommendation'),
    device_type       ENUM('mobile','desktop','smart_speaker','web'),
    country           VARCHAR(50),
    FOREIGN KEY (user_id)  REFERENCES users(user_id),
    FOREIGN KEY (track_id) REFERENCES tracks(track_id)
);