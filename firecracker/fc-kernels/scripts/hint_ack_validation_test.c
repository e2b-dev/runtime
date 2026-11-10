/* Unit harness for the added hint-ACK dependency, not upstream validate(). */
#include <assert.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>

#define VIRTIO_BALLOON_F_FREE_PAGE_HINT 3
#define VIRTIO_BALLOON_F_PAGE_POISON 4
#define VIRTIO_BALLOON_F_REPORTING 5
#define VIRTIO_BALLOON_F_HINT_WAIT_ON_ACK 6
#define VIRTIO_RING_F_INDIRECT_DESC 28
#define VIRTIO_F_ACCESS_PLATFORM 33

struct virtio_device {
    uint64_t features;
};

static bool virtio_has_feature(struct virtio_device *vdev, unsigned int bit)
{
    return (vdev->features & (UINT64_C(1) << bit)) != 0;
}

static void __virtio_clear_bit(struct virtio_device *vdev, unsigned int bit)
{
    vdev->features &= ~(UINT64_C(1) << bit);
}

static void validate_hint_ack(struct virtio_device *vdev)
{
    /* Generated from the carried patch; never duplicate its implementation. */
#include "hint_ack_validation.inc"
}

int main(void)
{
    const unsigned int bits[] = {
        VIRTIO_BALLOON_F_FREE_PAGE_HINT, VIRTIO_BALLOON_F_PAGE_POISON,
        VIRTIO_BALLOON_F_REPORTING, VIRTIO_BALLOON_F_HINT_WAIT_ON_ACK,
        VIRTIO_RING_F_INDIRECT_DESC, VIRTIO_F_ACCESS_PLATFORM,
    };
    const unsigned int count = sizeof(bits) / sizeof(bits[0]);
    uint64_t tested = 0;

    for (unsigned int i = 0; i < count; i++)
        tested |= UINT64_C(1) << bits[i];

    /* Vary the relevant flags, with all other bits both clear and set. */
    for (unsigned int others = 0; others < 2; others++) {
        for (unsigned int mask = 0; mask < (1U << count); mask++) {
            uint64_t initial = others ? ~tested : 0;
            for (unsigned int i = 0; i < count; i++)
                if (mask & (1U << i))
                    initial |= UINT64_C(1) << bits[i];

            uint64_t expected = initial;
            if (!(initial & (UINT64_C(1) << VIRTIO_BALLOON_F_FREE_PAGE_HINT)))
                expected &= ~(UINT64_C(1) << VIRTIO_BALLOON_F_HINT_WAIT_ON_ACK);

            struct virtio_device vdev = {initial};
            validate_hint_ack(&vdev);
            assert(vdev.features == expected);
        }
    }

    puts("hint-ACK dependency: 128 feature masks passed");
    return 0;
}
